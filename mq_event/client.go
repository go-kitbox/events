package pulsarrpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

var (
	ErrTimeout      = errors.New("rpc请求超时")
	ErrMaxRetries   = errors.New("已达到最大重试次数")
	ErrNoResponse   = errors.New("未收到响应")
	ErrClientClosed = errors.New("客户端已关闭")
)

// CallResult RPC调用结果
// 支持channel风格返回
type CallResult struct {
	Response *Message
	Error    error
}

// pendingCall 等待中的调用
type pendingCall struct {
	ch            chan *CallResult
	deadline      time.Time
	startTime     time.Time
	correlationID string
}

// Client RPC客户端
// 封装了Pulsar生产者和消费者，提供同步和异步调用能力
type Client struct {
	config   *Config
	client   pulsar.Client
	producer pulsar.Producer
	consumer pulsar.Consumer

	recorder MessageRecorder

	pending sync.Map
	mu      sync.RWMutex
	closed  bool

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewClient 创建新的RPC客户端
// 参数:
//   - config: 配置实例
//
// 返回值:
//   - *Client: 客户端实例
//   - error: 创建失败时返回错误
func NewClient(config *Config) (*Client, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	clientOpts := pulsar.ClientOptions{
		URL: config.Pulsar.URL,
	}

	if config.Pulsar.Auth != nil {
		switch config.Pulsar.Auth.Type {
		case "token":
			clientOpts.Authentication = pulsar.NewAuthenticationToken(config.Pulsar.Auth.Token)
		}
	}

	if config.Pulsar.TLS != nil && config.Pulsar.TLS.Enabled {
		clientOpts.TLSTrustCertsFilePath = config.Pulsar.TLS.TrustCertsFile
		clientOpts.TLSAllowInsecureConnection = config.Pulsar.TLS.AllowInsecure
	}

	client, err := pulsar.NewClient(clientOpts)
	if err != nil {
		return nil, fmt.Errorf("创建Pulsar客户端失败: %w", err)
	}

	producerOpts := pulsar.ProducerOptions{
		Topic:                   config.Pulsar.Topic,
		CompressionType:         toPulsarCompression(config.Pulsar.Compression),
		BatchingMaxMessages:     uint(config.Pulsar.BatchMaxSize),
		BatchingMaxPublishDelay: time.Millisecond * 10,
	}

	producer, err := client.CreateProducer(producerOpts)
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("创建生产者失败: %w", err)
	}

	consumerOpts := pulsar.ConsumerOptions{
		Topic:            config.Pulsar.ReplyTopic,
		SubscriptionName: config.Pulsar.Subscription + "-reply",
		Type:             pulsar.Exclusive,
	}

	consumer, err := client.Subscribe(consumerOpts)
	if err != nil {
		producer.Close()
		client.Close()
		return nil, fmt.Errorf("创建消费者失败: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	c := &Client{
		config:   config,
		client:   client,
		producer: producer,
		consumer: consumer,
		recorder: NoopMessageRecorder(),
		ctx:      ctx,
		cancel:   cancel,
	}

	if config.Recorder.Enabled && config.Database.DSN != "" {
		recorder, err := NewMessageRecorderWithConfig(config.Database)
		if err != nil {
			producer.Close()
			consumer.Close()
			client.Close()
			return nil, fmt.Errorf("创建消息记录器失败: %w", err)
		}
		c.recorder = recorder
	}

	c.wg.Add(1)
	go c.replyListener()

	return c, nil
}

// NewClientWithRecorder 创建带自定义记录器的RPC客户端
// 参数:
//   - config: 配置实例
//   - recorder: 消息记录器
//
// 返回值:
//   - *Client: 客户端实例
//   - error: 创建失败时返回错误
func NewClientWithRecorder(config *Config, recorder MessageRecorder) (*Client, error) {
	c, err := NewClient(config)
	if err != nil {
		return nil, err
	}
	if recorder != nil {
		c.recorder = recorder
	}
	return c, nil
}

// SetRecorder 设置消息记录器
func (c *Client) SetRecorder(recorder MessageRecorder) {
	if recorder != nil {
		c.recorder = recorder
	}
}

// replyListener 监听响应消息的goroutine
func (c *Client) replyListener() {
	defer c.wg.Done()

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
			ctx, cancel := context.WithTimeout(c.ctx, 100*time.Millisecond)
			msg, err := c.consumer.Receive(ctx)
			cancel()

			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
					continue
				}
				continue
			}

			c.consumer.Ack(msg)

			rpcMsg, err := DecodeMessage(msg.Payload())
			if err != nil {
				continue
			}

			if val, ok := c.pending.Load(rpcMsg.CorrelationID); ok {
				pending := val.(*pendingCall)
				result := &CallResult{
					Response: rpcMsg,
				}
				if rpcMsg.Error != "" {
					result.Error = errors.New(rpcMsg.Error)
				}

				if c.config.Recorder.Enabled {
					processingTime := time.Since(pending.startTime).Milliseconds()
					respData, _ := json.Marshal(rpcMsg)
					if result.Error != nil {
						c.recorder.MarkFailed(pending.correlationID, result.Error.Error())
					} else {
						c.recorder.MarkSuccess(pending.correlationID, respData, processingTime)
					}
				}

				select {
				case pending.ch <- result:
				default:
				}
				c.pending.Delete(rpcMsg.CorrelationID)
			}
		}
	}
}

// Call 同步RPC调用
// 参数:
//   - ctx: 上下文，用于控制超时和取消
//   - payload: 请求负载数据
//
// 返回值:
//   - *Message: 响应消息
//   - error: 调用失败时返回错误
func (c *Client) Call(ctx context.Context, payload interface{}) (*Message, error) {
	results := c.CallAsync(ctx, payload)
	select {
	case result := <-results:
		return result.Response, result.Error
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// CallAsync 异步RPC调用（channel风格）
// 参数:
//   - ctx: 上下文，用于控制超时
//   - payload: 请求负载数据
//
// 返回值:
//   - <-chan *CallResult: 结果channel，调用方从中读取结果
func (c *Client) CallAsync(ctx context.Context, payload interface{}) <-chan *CallResult {
	resultCh := make(chan *CallResult, 1)

	go func() {
		defer close(resultCh)

		c.mu.RLock()
		if c.closed {
			c.mu.RUnlock()
			resultCh <- &CallResult{Error: ErrClientClosed}
			return
		}
		c.mu.RUnlock()

		var msg *Message
		var err error

		switch v := payload.(type) {
		case *Message:
			msg = v
		default:
			msg, err = NewMessage(payload)
			if err != nil {
				resultCh <- &CallResult{Error: err}
				return
			}
		}

		var lastErr error
		for retry := 0; retry <= c.config.RPC.MaxRetries; retry++ {
			if retry > 0 {
				select {
				case <-ctx.Done():
					resultCh <- &CallResult{Error: ctx.Err()}
					return
				case <-time.After(c.config.RPC.RetryDelay):
				}
			}

			msg.RetryCount = retry

			response, err := c.doCall(ctx, msg)
			if err != nil {
				lastErr = err
				if errors.Is(err, ErrTimeout) || errors.Is(err, context.DeadlineExceeded) {
					continue
				}
				resultCh <- &CallResult{Error: err}
				return
			}

			resultCh <- &CallResult{Response: response}
			return
		}

		resultCh <- &CallResult{Error: fmt.Errorf("%w: %v", ErrMaxRetries, lastErr)}
	}()

	return resultCh
}

// doCall 执行单次RPC调用
func (c *Client) doCall(ctx context.Context, msg *Message) (*Message, error) {
	data, err := msg.Encode()
	if err != nil {
		return nil, err
	}

	startTime := time.Now()

	pending := &pendingCall{
		ch:            make(chan *CallResult, 1),
		deadline:      time.Now().Add(c.config.RPC.Timeout),
		startTime:     startTime,
		correlationID: msg.CorrelationID,
	}
	c.pending.Store(msg.CorrelationID, pending)
	defer c.pending.Delete(msg.CorrelationID)

	if c.config.Recorder.Enabled {
		record := &MessageRecord{
			CorrelationID: msg.CorrelationID,
			Type:          msg.Type,
			SubType:       msg.SubType,
			Direction:     DirectionSend,
			Status:        StatusPending,
			Payload:       data,
			RetryCount:    msg.RetryCount,
			Source:        c.config.Recorder.Source,
			Destination:   c.config.Pulsar.Topic,
		}
		if msg.Metadata != nil {
			metaData, _ := json.Marshal(msg.Metadata)
			record.Metadata = string(metaData)
		}
		go c.recorder.Record(record)
	}

	_, err = c.producer.Send(ctx, &pulsar.ProducerMessage{
		Payload:   data,
		EventTime: time.Now(),
	})
	if err != nil {
		if c.config.Recorder.Enabled {
			c.recorder.MarkFailed(msg.CorrelationID, fmt.Sprintf("发送消息失败: %v", err))
		}
		return nil, fmt.Errorf("发送消息失败: %w", err)
	}

	timeout := time.Until(pending.deadline)
	if timeout <= 0 {
		if c.config.Recorder.Enabled {
			c.recorder.MarkTimeout(msg.CorrelationID)
		}
		return nil, ErrTimeout
	}

	select {
	case result := <-pending.ch:
		return result.Response, result.Error
	case <-time.After(timeout):
		if c.config.Recorder.Enabled {
			c.recorder.MarkTimeout(msg.CorrelationID)
		}
		return nil, ErrTimeout
	case <-ctx.Done():
		if c.config.Recorder.Enabled {
			c.recorder.MarkFailed(msg.CorrelationID, ctx.Err().Error())
		}
		return nil, ctx.Err()
	}
}

// Close 关闭客户端
// 等待所有goroutine完成并释放资源
func (c *Client) Close() error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()

	c.cancel()
	c.wg.Wait()

	if c.producer != nil {
		c.producer.Close()
	}
	if c.consumer != nil {
		c.consumer.Close()
	}
	if c.client != nil {
		c.client.Close()
	}

	return nil
}

// toPulsarCompression 转换压缩类型到Pulsar压缩类型
func toPulsarCompression(ct CompressionType) pulsar.CompressionType {
	switch ct {
	case CompressionLZ4:
		return pulsar.LZ4
	case CompressionZLib:
		return pulsar.ZLib
	case CompressionZSTD:
		return pulsar.ZSTD
	default:
		return pulsar.NoCompression
	}
}
