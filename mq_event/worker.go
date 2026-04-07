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

// Handler 消息处理函数类型
// 参数:
//   - ctx: 上下文
//   - msg: 接收到的RPC消息
//
// 返回值:
//   - interface{}: 响应数据
//   - error: 处理错误
type Handler func(ctx context.Context, msg *Message) (interface{}, error)

// Worker RPC工作器
// 负责从Pulsar消费请求消息并调用处理函数
type Worker struct {
	config   *Config
	client   pulsar.Client
	consumer pulsar.Consumer
	producer pulsar.Producer
	handler  Handler
	recorder MessageRecorder

	workers int
	taskCh  chan pulsar.Message

	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	mu      sync.RWMutex
	running bool
}

// NewWorker 创建新的Worker实例
// 参数:
//   - config: 配置实例
//   - handler: 消息处理函数
//
// 返回值:
//   - *Worker: Worker实例
//   - error: 创建失败时返回错误
func NewWorker(config *Config, handler Handler) (*Worker, error) {
	if handler == nil {
		return nil, errors.New("handler不能为空")
	}
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

	consumerOpts := pulsar.ConsumerOptions{
		Topic:            config.Pulsar.Topic,
		SubscriptionName: config.Pulsar.Subscription,
		Type:             pulsar.Shared,
	}

	consumer, err := client.Subscribe(consumerOpts)
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("创建消费者失败: %w", err)
	}

	producerOpts := pulsar.ProducerOptions{
		Topic: config.Pulsar.ReplyTopic,
	}
	producerOpts.CompressionType = toPulsarCompression(config.Pulsar.Compression)

	producer, err := client.CreateProducer(producerOpts)
	if err != nil {
		consumer.Close()
		client.Close()
		return nil, fmt.Errorf("创建生产者失败: %w", err)
	}

	workers := config.RPC.WorkerCount
	if workers <= 0 {
		workers = 10
	}

	ctx, cancel := context.WithCancel(context.Background())

	w := &Worker{
		config:   config,
		client:   client,
		consumer: consumer,
		producer: producer,
		handler:  handler,
		recorder: NoopMessageRecorder(),
		workers:  workers,
		taskCh:   make(chan pulsar.Message, config.RPC.QueueSize),
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
		w.recorder = recorder
	}

	return w, nil
}

// NewWorkerWithRecorder 创建带自定义记录器的Worker实例
// 参数:
//   - config: 配置实例
//   - handler: 消息处理函数
//   - recorder: 消息记录器
//
// 返回值:
//   - *Worker: Worker实例
//   - error: 创建失败时返回错误
func NewWorkerWithRecorder(config *Config, handler Handler, recorder MessageRecorder) (*Worker, error) {
	w, err := NewWorker(config, handler)
	if err != nil {
		return nil, err
	}
	if recorder != nil {
		w.recorder = recorder
	}
	return w, nil
}

// SetRecorder 设置消息记录器
func (w *Worker) SetRecorder(recorder MessageRecorder) {
	if recorder != nil {
		w.recorder = recorder
	}
}

// Start 启动Worker
// 启动指定数量的goroutine处理消息
func (w *Worker) Start() {
	w.mu.Lock()
	if w.running {
		w.mu.Unlock()
		return
	}
	w.running = true
	w.mu.Unlock()

	for i := 0; i < w.workers; i++ {
		w.wg.Add(1)
		go w.worker(i)
	}

	w.wg.Add(1)
	go w.dispatch()

	fmt.Printf("[Worker] 已启动 %d 个工作协程\n", w.workers)
}

// dispatch 消息分发器
func (w *Worker) dispatch() {
	defer w.wg.Done()

	for {
		select {
		case <-w.ctx.Done():
			return
		default:
			ctx, cancel := context.WithTimeout(w.ctx, 100*time.Millisecond)
			msg, err := w.consumer.Receive(ctx)
			cancel()

			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
					continue
				}
				continue
			}

			select {
			case w.taskCh <- msg:
			case <-w.ctx.Done():
				w.consumer.Nack(msg)
				return
			}
		}
	}
}

// worker 工作协程
func (w *Worker) worker(id int) {
	defer w.wg.Done()

	for {
		select {
		case <-w.ctx.Done():
			return
		case msg := <-w.taskCh:
			w.processMessage(msg, id)
		}
	}
}

// processMessage 处理单条消息
func (w *Worker) processMessage(msg pulsar.Message, workerID int) {
	ctx, cancel := context.WithTimeout(w.ctx, w.config.RPC.Timeout)
	defer cancel()

	startTime := time.Now()

	rpcMsg, err := DecodeMessage(msg.Payload())
	if err != nil {
		w.consumer.Nack(msg)
		fmt.Printf("[Worker-%d] 消息解码失败: %v\n", workerID, err)
		return
	}

	if w.config.Recorder.Enabled {
		record := &MessageRecord{
			CorrelationID: rpcMsg.CorrelationID,
			Type:          rpcMsg.Type,
			SubType:       rpcMsg.SubType,
			Direction:     DirectionReceive,
			Status:        StatusPending,
			Payload:       msg.Payload(),
			RetryCount:    rpcMsg.RetryCount,
			Source:        w.config.Pulsar.Topic,
			Destination:   w.config.Recorder.Source,
		}
		if rpcMsg.Metadata != nil {
			metaData, _ := json.Marshal(rpcMsg.Metadata)
			record.Metadata = string(metaData)
		}
		go w.recorder.Record(record)
	}

	response, err := w.handler(ctx, rpcMsg)
	if err != nil {
		errMsg := CreateErrorResponse(rpcMsg.CorrelationID, err.Error())
		if sendErr := w.sendResponse(errMsg); sendErr != nil {
			fmt.Printf("[Worker-%d] 发送错误响应失败: %v\n", workerID, sendErr)
		}
		if w.config.Recorder.Enabled {
			w.recorder.MarkFailed(rpcMsg.CorrelationID, err.Error())
		}
		w.consumer.Ack(msg)
		return
	}

	respMsg, err := CreateResponse(rpcMsg.CorrelationID, response)
	if err != nil {
		errMsg := CreateErrorResponse(rpcMsg.CorrelationID, err.Error())
		if sendErr := w.sendResponse(errMsg); sendErr != nil {
			fmt.Printf("[Worker-%d] 发送错误响应失败: %v\n", workerID, sendErr)
		}
		if w.config.Recorder.Enabled {
			w.recorder.MarkFailed(rpcMsg.CorrelationID, err.Error())
		}
		w.consumer.Ack(msg)
		return
	}

	if err := w.sendResponse(respMsg); err != nil {
		fmt.Printf("[Worker-%d] 发送响应失败: %v\n", workerID, err)
		if w.config.Recorder.Enabled {
			w.recorder.MarkFailed(rpcMsg.CorrelationID, fmt.Sprintf("发送响应失败: %v", err))
		}
		w.consumer.Nack(msg)
		return
	}

	if w.config.Recorder.Enabled {
		processingTime := time.Since(startTime).Milliseconds()
		respData, _ := json.Marshal(respMsg)
		w.recorder.MarkSuccess(rpcMsg.CorrelationID, respData, processingTime)
	}

	w.consumer.Ack(msg)
}

// sendResponse 发送响应消息
func (w *Worker) sendResponse(msg *Message) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = w.producer.Send(ctx, &pulsar.ProducerMessage{
		Payload:   data,
		EventTime: time.Now(),
	})
	return err
}

// Stop 停止Worker
// 等待所有工作协程完成并释放资源
func (w *Worker) Stop() error {
	w.mu.Lock()
	if !w.running {
		w.mu.Unlock()
		return nil
	}
	w.running = false
	w.mu.Unlock()

	w.cancel()
	w.wg.Wait()

	close(w.taskCh)

	if w.consumer != nil {
		w.consumer.Close()
	}
	if w.producer != nil {
		w.producer.Close()
	}
	if w.client != nil {
		w.client.Close()
	}

	fmt.Println("[Worker] 已停止")
	return nil
}

// SetHandler 设置消息处理函数
// 允许在运行时更换处理函数
func (w *Worker) SetHandler(handler Handler) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.handler = handler
}
