package pulsarrpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/google/uuid"
)

// EventType 事件类型枚举
type EventType string

const (
	EventTypeUserCalc   EventType = "user_calc"   // 用户计算事件
	EventTypeUserNotify EventType = "user_notify" // 用户通知事件
	EventTypeSystemTask EventType = "system_task" // 系统任务事件
	EventTypeBroadcast  EventType = "broadcast"   // 广播事件
	EventTypeCustom     EventType = "custom"      // 自定义事件
)

// Event 分布式事件结构体
// 通过Pulsar广播到所有节点
type Event struct {
	EventID       string            `json:"event_id"`       // 事件唯一ID
	Type          EventType         `json:"type"`           // 事件类型
	SubType       string            `json:"sub_type"`       // 事件子类型
	UID           int64             `json:"uid"`            // 用户ID，0表示系统
	Source        string            `json:"source"`         // 事件来源节点
	Payload       json.RawMessage   `json:"payload"`        // 事件负载数据
	Timestamp     int64             `json:"timestamp"`      // 事件时间戳（纳秒）
	Metadata      map[string]string `json:"metadata"`       // 扩展元数据
	CorrelationID string            `json:"correlation_id"` // 关联ID（可选）
}

// EventHandler 事件处理函数类型
type EventHandler func(ctx context.Context, event *Event) error

// EventBus 分布式事件总线
// 基于Pulsar实现多节点事件广播
type EventBus struct {
	config   *Config
	client   pulsar.Client
	producer pulsar.Producer
	consumer pulsar.Consumer

	handlers map[EventType]map[string]EventHandler
	mu       sync.RWMutex

	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	running bool

	nodeID string
}

// EventBusConfig 事件总线配置
type EventBusConfig struct {
	Topic        string `json:"topic" yaml:"topic"`               // 事件主题
	Subscription string `json:"subscription" yaml:"subscription"` // 订阅名称
}

// NewEvent 创建新事件
// 参数:
//   - eventType: 事件类型
//   - payload: 事件负载（任意类型，会被序列化为JSON）
//
// 返回值:
//   - *Event: 新创建的事件实例
//   - error: 创建失败时返回错误
func NewEvent(eventType EventType, payload interface{}) (*Event, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return &Event{
		EventID:   uuid.New().String(),
		Type:      eventType,
		Payload:   data,
		Timestamp: time.Now().UnixNano(),
		Metadata:  make(map[string]string),
	}, nil
}

// NewEventWithSubType 创建带子类型的事件
func NewEventWithSubType(eventType EventType, subType string, payload interface{}) (*Event, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return &Event{
		EventID:   uuid.New().String(),
		Type:      eventType,
		SubType:   subType,
		Payload:   data,
		Timestamp: time.Now().UnixNano(),
		Metadata:  make(map[string]string),
	}, nil
}

// ParsePayload 解析事件负载
func (e *Event) ParsePayload(target interface{}) error {
	return json.Unmarshal(e.Payload, target)
}

// WithUID 设置用户ID
func (e *Event) WithUID(uid int64) *Event {
	e.UID = uid
	return e
}

// WithSource 设置来源
func (e *Event) WithSource(source string) *Event {
	e.Source = source
	return e
}

// WithMetadata 设置元数据
func (e *Event) WithMetadata(key, value string) *Event {
	if e.Metadata == nil {
		e.Metadata = make(map[string]string)
	}
	e.Metadata[key] = value
	return e
}

// WithCorrelationID 设置关联ID
func (e *Event) WithCorrelationID(id string) *Event {
	e.CorrelationID = id
	return e
}

// Encode 编码事件为字节数组
func (e *Event) Encode() ([]byte, error) {
	return json.Marshal(e)
}

// DecodeEvent 从字节数组解码事件
func DecodeEvent(data []byte) (*Event, error) {
	var event Event
	if err := json.Unmarshal(data, &event); err != nil {
		return nil, err
	}
	return &event, nil
}

// NewEventBus 创建事件总线
// 参数:
//   - config: RPC配置
//   - eventTopic: 事件主题
//   - subscription: 订阅名称
//
// 返回值:
//   - *EventBus: 事件总线实例
//   - error: 创建失败时返回错误
func NewEventBus(config *Config, eventTopic, subscription string) (*EventBus, error) {
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
		Topic:           eventTopic,
		CompressionType: toPulsarCompression(config.Pulsar.Compression),
	}

	producer, err := client.CreateProducer(producerOpts)
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("创建事件生产者失败: %w", err)
	}

	consumerOpts := pulsar.ConsumerOptions{
		Topic:            eventTopic,
		SubscriptionName: subscription,
		Type:             pulsar.Shared,
	}

	consumer, err := client.Subscribe(consumerOpts)
	if err != nil {
		producer.Close()
		client.Close()
		return nil, fmt.Errorf("创建事件消费者失败: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	nodeID := uuid.New().String()[:8]

	bus := &EventBus{
		config:   config,
		client:   client,
		producer: producer,
		consumer: consumer,
		handlers: make(map[EventType]map[string]EventHandler),
		ctx:      ctx,
		cancel:   cancel,
		nodeID:   nodeID,
	}

	return bus, nil
}

// NodeID 获取当前节点ID
func (bus *EventBus) NodeID() string {
	return bus.nodeID
}

// On 注册事件监听器
// 参数:
//   - eventType: 事件类型
//   - handler: 事件处理函数
//
// 返回值:
//   - string: 监听器ID，用于取消注册
func (bus *EventBus) On(eventType EventType, handler EventHandler) string {
	bus.mu.Lock()
	defer bus.mu.Unlock()

	listenerID := fmt.Sprintf("%s-%d", bus.nodeID, time.Now().UnixNano())
	if bus.handlers[eventType] == nil {
		bus.handlers[eventType] = make(map[string]EventHandler)
	}
	bus.handlers[eventType][listenerID] = handler
	return listenerID
}

// Once 注册一次性事件监听器
func (bus *EventBus) Once(eventType EventType, handler EventHandler) string {
	bus.mu.Lock()
	defer bus.mu.Unlock()

	listenerID := fmt.Sprintf("%s-%d-once", bus.nodeID, time.Now().UnixNano())
	wrapper := func(ctx context.Context, event *Event) error {
		err := handler(ctx, event)
		bus.mu.Lock()
		delete(bus.handlers[eventType], listenerID)
		bus.mu.Unlock()
		return err
	}

	if bus.handlers[eventType] == nil {
		bus.handlers[eventType] = make(map[string]EventHandler)
	}
	bus.handlers[eventType][listenerID] = wrapper
	return listenerID
}

// Off 移除事件监听器
func (bus *EventBus) Off(listenerID string) {
	bus.mu.Lock()
	defer bus.mu.Unlock()

	for eventType := range bus.handlers {
		delete(bus.handlers[eventType], listenerID)
	}
}

// Emit 发布事件（异步）
// 事件会被广播到所有订阅的节点
func (bus *EventBus) Emit(ctx context.Context, event *Event) error {
	if event.Source == "" {
		event.Source = bus.nodeID
	}
	if event.Timestamp == 0 {
		event.Timestamp = time.Now().UnixNano()
	}

	data, err := event.Encode()
	if err != nil {
		return err
	}

	_, err = bus.producer.Send(ctx, &pulsar.ProducerMessage{
		Payload:   data,
		EventTime: time.Now(),
	})
	return err
}

// EmitSync 发布事件（同步，等待确认）
func (bus *EventBus) EmitSync(ctx context.Context, event *Event) error {
	return bus.Emit(ctx, event)
}

// EmitAsync 发布事件（完全异步，不等待确认）
func (bus *EventBus) EmitAsync(event *Event) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return bus.Emit(ctx, event)
}

// Start 启动事件总线
// 开始监听事件
func (bus *EventBus) Start() {
	bus.mu.Lock()
	if bus.running {
		bus.mu.Unlock()
		return
	}
	bus.running = true
	bus.mu.Unlock()

	bus.wg.Add(1)
	go bus.eventListener()

	fmt.Printf("[EventBus] 节点 %s 已启动事件监听\n", bus.nodeID)
}

// eventListener 事件监听goroutine
func (bus *EventBus) eventListener() {
	defer bus.wg.Done()

	for {
		select {
		case <-bus.ctx.Done():
			return
		default:
			ctx, cancel := context.WithTimeout(bus.ctx, 100*time.Millisecond)
			msg, err := bus.consumer.Receive(ctx)
			cancel()

			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
					continue
				}
				continue
			}

			bus.processEvent(msg)
		}
	}
}

// processEvent 处理单个事件
func (bus *EventBus) processEvent(msg pulsar.Message) {
	event, err := DecodeEvent(msg.Payload())
	if err != nil {
		bus.consumer.Ack(msg)
		fmt.Printf("[EventBus] 事件解码失败: %v\n", err)
		return
	}

	bus.mu.RLock()
	handlers := bus.handlers[event.Type]
	bus.mu.RUnlock()

	if len(handlers) == 0 {
		bus.consumer.Ack(msg)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for _, handler := range handlers {
		if err := handler(ctx, event); err != nil {
			fmt.Printf("[EventBus] 事件处理失败: type=%s, event_id=%s, error=%v\n",
				event.Type, event.EventID, err)
		}
	}

	bus.consumer.Ack(msg)
}

// Stop 停止事件总线
func (bus *EventBus) Stop() error {
	bus.mu.Lock()
	if !bus.running {
		bus.mu.Unlock()
		return nil
	}
	bus.running = false
	bus.mu.Unlock()

	bus.cancel()
	bus.wg.Wait()

	if bus.producer != nil {
		bus.producer.Close()
	}
	if bus.consumer != nil {
		bus.consumer.Close()
	}
	if bus.client != nil {
		bus.client.Close()
	}

	fmt.Printf("[EventBus] 节点 %s 已停止\n", bus.nodeID)
	return nil
}

// Subscribe 订阅特定事件类型（On的别名，语义更清晰）
func (bus *EventBus) Subscribe(eventType EventType, handler EventHandler) string {
	return bus.On(eventType, handler)
}

// Unsubscribe 取消订阅（Off的别名）
func (bus *EventBus) Unsubscribe(listenerID string) {
	bus.Off(listenerID)
}

// Publish 发布事件（Emit的别名）
func (bus *EventBus) Publish(ctx context.Context, event *Event) error {
	return bus.Emit(ctx, event)
}

// HasHandlers 检查是否有注册的处理器
func (bus *EventBus) HasHandlers(eventType EventType) bool {
	bus.mu.RLock()
	defer bus.mu.RUnlock()
	return len(bus.handlers[eventType]) > 0
}

// HandlerCount 获取指定事件类型的处理器数量
func (bus *EventBus) HandlerCount(eventType EventType) int {
	bus.mu.RLock()
	defer bus.mu.RUnlock()
	return len(bus.handlers[eventType])
}
