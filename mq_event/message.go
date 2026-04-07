package pulsarrpc

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// MessageType 消息类型枚举
type MessageType string

const (
	MessageTypeRequest  MessageType = "request"  // 请求消息
	MessageTypeResponse MessageType = "response" // 响应消息
	MessageTypeError    MessageType = "error"    // 错误消息
)

// Message RPC消息结构体
// 用于封装请求和响应的核心数据结构
type Message struct {
	CorrelationID string            `json:"correlation_id"`     // 关联ID，用于请求响应匹配
	Type          MessageType       `json:"type"`               // 消息类型: request/response/error
	SubType       string            `json:"sub_type,omitempty"` // 消息子类型，用于区分具体业务类型
	Payload       json.RawMessage   `json:"payload"`            // 消息负载，使用RawMessage保留原始JSON
	Timestamp     int64             `json:"timestamp"`          // 消息时间戳（纳秒）
	Error         string            `json:"error,omitempty"`    // 错误信息（响应时使用）
	RetryCount    int               `json:"retry_count"`        // 当前重试次数
	Metadata      map[string]string `json:"metadata,omitempty"` // 扩展元数据
}

// NewMessage 创建新的RPC消息
// 参数:
//   - payload: 消息负载数据（任意类型，会被序列化为JSON）
//
// 返回值:
//   - *Message: 新创建的消息实例
//   - error: 序列化失败时返回错误
func NewMessage(payload interface{}) (*Message, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return &Message{
		CorrelationID: uuid.New().String(),
		Type:          MessageTypeRequest,
		Payload:       data,
		Timestamp:     time.Now().UnixNano(),
		RetryCount:    0,
		Metadata:      make(map[string]string),
	}, nil
}

// NewMessageWithType 创建带类型和子类型的RPC消息
// 参数:
//   - msgType: 消息类型
//   - subType: 消息子类型
//   - payload: 消息负载数据
//
// 返回值:
//   - *Message: 新创建的消息实例
//   - error: 序列化失败时返回错误
func NewMessageWithType(msgType MessageType, subType string, payload interface{}) (*Message, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return &Message{
		CorrelationID: uuid.New().String(),
		Type:          msgType,
		SubType:       subType,
		Payload:       data,
		Timestamp:     time.Now().UnixNano(),
		RetryCount:    0,
		Metadata:      make(map[string]string),
	}, nil
}

// Encode 将消息编码为字节数组
// 返回值:
//   - []byte: 编码后的字节数组
//   - error: 编码失败时返回错误
func (m *Message) Encode() ([]byte, error) {
	return json.Marshal(m)
}

// DecodeMessage 从字节数组解码消息
// 参数:
//   - data: 待解码的字节数组
//
// 返回值:
//   - *Message: 解码后的消息实例
//   - error: 解码失败时返回错误
func DecodeMessage(data []byte) (*Message, error) {
	var msg Message
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, err
	}
	return &msg, nil
}

// ParsePayload 解析消息负载到目标对象
// 参数:
//   - target: 目标对象指针，用于接收解析结果
//
// 返回值:
//   - error: 解析失败时返回错误
func (m *Message) ParsePayload(target interface{}) error {
	return json.Unmarshal(m.Payload, target)
}

// ParsePayloadBySubType 根据子类型解析消息负载
// 参数:
//   - subType: 期望的子类型
//   - target: 目标对象指针
//
// 返回值:
//   - error: 子类型不匹配或解析失败时返回错误
func (m *Message) ParsePayloadBySubType(subType string, target interface{}) error {
	if m.SubType != subType {
		return fmt.Errorf("子类型不匹配: 期望 %s, 实际 %s", subType, m.SubType)
	}
	return json.Unmarshal(m.Payload, target)
}

// ParsePayloadByType 根据消息类型解析消息负载
// 参数:
//   - msgType: 期望的消息类型
//   - target: 目标对象指针
//
// 返回值:
//   - error: 类型不匹配或解析失败时返回错误
func (m *Message) ParsePayloadByType(msgType MessageType, target interface{}) error {
	if m.Type != msgType {
		return fmt.Errorf("消息类型不匹配: 期望 %s, 实际 %s", msgType, m.Type)
	}
	return json.Unmarshal(m.Payload, target)
}

// SetPayload 设置消息负载
// 参数:
//   - payload: 负载数据（任意类型）
//
// 返回值:
//   - error: 序列化失败时返回错误
func (m *Message) SetPayload(payload interface{}) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	m.Payload = data
	return nil
}

// WithError 为消息设置错误信息
// 参数:
//   - errMsg: 错误信息字符串
//
// 返回值:
//   - *Message: 返回消息本身，支持链式调用
func (m *Message) WithError(errMsg string) *Message {
	m.Error = errMsg
	m.Type = MessageTypeError
	return m
}

// WithType 设置消息类型
// 参数:
//   - msgType: 消息类型
//
// 返回值:
//   - *Message: 返回消息本身，支持链式调用
func (m *Message) WithType(msgType MessageType) *Message {
	m.Type = msgType
	return m
}

// WithSubType 设置消息子类型
// 参数:
//   - subType: 消息子类型
//
// 返回值:
//   - *Message: 返回消息本身，支持链式调用
func (m *Message) WithSubType(subType string) *Message {
	m.SubType = subType
	return m
}

// WithMetadata 设置元数据
// 参数:
//   - key: 元数据键
//   - value: 元数据值
//
// 返回值:
//   - *Message: 返回消息本身，支持链式调用
func (m *Message) WithMetadata(key, value string) *Message {
	if m.Metadata == nil {
		m.Metadata = make(map[string]string)
	}
	m.Metadata[key] = value
	return m
}

// IncrementRetry 增加重试计数
// 返回值:
//   - int: 新的重试次数
func (m *Message) IncrementRetry() int {
	m.RetryCount++
	return m.RetryCount
}

// IsRetryable 判断消息是否可以重试
// 参数:
//   - maxRetries: 最大重试次数
//
// 返回值:
//   - bool: true表示可以重试，false表示已达到最大重试次数
func (m *Message) IsRetryable(maxRetries int) bool {
	return m.RetryCount < maxRetries
}

// IsError 判断消息是否为错误消息
// 返回值:
//   - bool: true表示是错误消息
func (m *Message) IsError() bool {
	return m.Type == MessageTypeError || m.Error != ""
}

// CreateResponse 创建响应消息
// 参数:
//   - correlationID: 原始请求的关联ID
//   - payload: 响应负载数据
//
// 返回值:
//   - *Message: 新创建的响应消息
//   - error: 创建失败时返回错误
func CreateResponse(correlationID string, payload interface{}) (*Message, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return &Message{
		CorrelationID: correlationID,
		Type:          MessageTypeResponse,
		Payload:       data,
		Timestamp:     time.Now().UnixNano(),
		RetryCount:    0,
		Metadata:      make(map[string]string),
	}, nil
}

// CreateResponseWithSubType 创建带子类型的响应消息
// 参数:
//   - correlationID: 原始请求的关联ID
//   - subType: 消息子类型
//   - payload: 响应负载数据
//
// 返回值:
//   - *Message: 新创建的响应消息
//   - error: 创建失败时返回错误
func CreateResponseWithSubType(correlationID string, subType string, payload interface{}) (*Message, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return &Message{
		CorrelationID: correlationID,
		Type:          MessageTypeResponse,
		SubType:       subType,
		Payload:       data,
		Timestamp:     time.Now().UnixNano(),
		RetryCount:    0,
		Metadata:      make(map[string]string),
	}, nil
}

// CreateErrorResponse 创建错误响应消息
// 参数:
//   - correlationID: 原始请求的关联ID
//   - errMsg: 错误信息
//
// 返回值:
//   - *Message: 新创建的错误响应消息
func CreateErrorResponse(correlationID string, errMsg string) *Message {
	return &Message{
		CorrelationID: correlationID,
		Type:          MessageTypeError,
		Payload:       nil,
		Timestamp:     time.Now().UnixNano(),
		Error:         errMsg,
		RetryCount:    0,
	}
}

// CreateErrorResponseWithSubType 创建带子类型的错误响应消息
// 参数:
//   - correlationID: 原始请求的关联ID
//   - subType: 消息子类型
//   - errMsg: 错误信息
//
// 返回值:
//   - *Message: 新创建的错误响应消息
func CreateErrorResponseWithSubType(correlationID string, subType string, errMsg string) *Message {
	return &Message{
		CorrelationID: correlationID,
		Type:          MessageTypeError,
		SubType:       subType,
		Payload:       nil,
		Timestamp:     time.Now().UnixNano(),
		Error:         errMsg,
		RetryCount:    0,
	}
}

// ========== 通用请求/响应结构体 ==========

// Request 通用请求结构体
// 可嵌入到具体业务请求中
type Request struct {
	Type    MessageType `json:"type"`     // 消息类型
	SubType string      `json:"sub_type"` // 消息子类型
}

// Response 通用响应结构体
// 可嵌入到具体业务响应中
type Response struct {
	Type    MessageType `json:"type"`              // 消息类型
	SubType string      `json:"sub_type"`          // 消息子类型
	Success bool        `json:"success"`           // 是否成功
	Message string      `json:"message,omitempty"` // 响应消息
}

// TypedPayload 带类型的负载包装器
// 用于在解析前获取类型信息
type TypedPayload struct {
	Type    MessageType     `json:"type"`
	SubType string          `json:"sub_type,omitempty"`
	Payload json.RawMessage `json:"payload"`
}

// ParseTypedPayload 解析带类型的负载
// 参数:
//   - data: 原始JSON数据
//
// 返回值:
//   - *TypedPayload: 类型化负载
//   - error: 解析失败时返回错误
func ParseTypedPayload(data []byte) (*TypedPayload, error) {
	var tp TypedPayload
	if err := json.Unmarshal(data, &tp); err != nil {
		return nil, err
	}
	return &tp, nil
}

// ParsePayloadFromTypedPayload 从TypedPayload解析具体负载
// 参数:
//   - tp: 类型化负载
//   - target: 目标对象指针
//
// 返回值:
//   - error: 解析失败时返回错误
func ParsePayloadFromTypedPayload(tp *TypedPayload, target interface{}) error {
	return json.Unmarshal(tp.Payload, target)
}

// SubTypeHandler 子类型处理器函数类型
type SubTypeHandler func(payload json.RawMessage) (interface{}, error)

// SubTypeRouter 子类型路由器
// 用于根据子类型分发到不同的处理函数
type SubTypeRouter struct {
	handlers map[string]SubTypeHandler
}

// NewSubTypeRouter 创建新的子类型路由器
func NewSubTypeRouter() *SubTypeRouter {
	return &SubTypeRouter{
		handlers: make(map[string]SubTypeHandler),
	}
}

// Register 注册子类型处理器
// 参数:
//   - subType: 子类型名称
//   - handler: 处理函数
func (r *SubTypeRouter) Register(subType string, handler SubTypeHandler) {
	r.handlers[subType] = handler
}

// Handle 处理消息
// 参数:
//   - msg: RPC消息
//
// 返回值:
//   - interface{}: 处理结果
//   - error: 处理失败或未找到处理器时返回错误
func (r *SubTypeRouter) Handle(msg *Message) (interface{}, error) {
	handler, ok := r.handlers[msg.SubType]
	if !ok {
		return nil, fmt.Errorf("未找到子类型 %s 的处理器", msg.SubType)
	}
	return handler(msg.Payload)
}

// HasHandler 检查是否注册了指定子类型的处理器
// 参数:
//   - subType: 子类型名称
//
// 返回值:
//   - bool: true表示已注册
func (r *SubTypeRouter) HasHandler(subType string) bool {
	_, ok := r.handlers[subType]
	return ok
}
