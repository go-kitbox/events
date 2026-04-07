package pulsarrpc

import (
	"fmt"
	"sync"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// DatabaseConfig 数据库配置
type DatabaseConfig struct {
	Driver   string `json:"driver" yaml:"driver"`       // 数据库驱动: mysql
	DSN      string `json:"dsn" yaml:"dsn"`             // 数据源名称
	LogLevel string `json:"log_level" yaml:"log_level"` // 日志级别: silent, error, warn, info
}

// MessageRecord 消息记录模型
// 用于存储RPC消息的发送和接收记录
type MessageRecord struct {
	ID             uint        `gorm:"primaryKey" json:"id"`
	CorrelationID  string      `gorm:"index;size:64;not null" json:"correlation_id"`
	Type           MessageType `gorm:"size:20;not null" json:"type"`
	SubType        string      `gorm:"size:100" json:"sub_type"`
	Direction      string      `gorm:"size:10;not null;index" json:"direction"` // send, receive
	Status         string      `gorm:"size:20;not null;index" json:"status"`    // pending, success, failed, timeout
	Payload        []byte      `gorm:"type:text" json:"payload"`
	Response       []byte      `gorm:"type:text" json:"response"`
	ErrorMsg       string      `gorm:"size:500" json:"error_msg"`
	RetryCount     int         `gorm:"default:0" json:"retry_count"`
	Source         string      `gorm:"size:100" json:"source"`      // 来源标识
	Destination    string      `gorm:"size:100" json:"destination"` // 目标标识
	ProcessingTime int64       `json:"processing_time"`             // 处理耗时（毫秒）
	CreatedAt      time.Time   `gorm:"index" json:"created_at"`
	UpdatedAt      time.Time   `json:"updated_at"`
	CompletedAt    *time.Time  `json:"completed_at"`
	Metadata       string      `gorm:"type:text" json:"metadata"` // JSON格式的扩展元数据
}

// TableName 指定表名
func (MessageRecord) TableName() string {
	return "pulsar_rpc_messages"
}

// MessageDirection 消息方向常量
const (
	DirectionSend    = "send"    // 发送
	DirectionReceive = "receive" // 接收
)

// MessageStatus 消息状态常量
const (
	StatusPending = "pending" // 处理中
	StatusSuccess = "success" // 成功
	StatusFailed  = "failed"  // 失败
	StatusTimeout = "timeout" // 超时
)

// MessageRecorder 消息记录器接口
type MessageRecorder interface {
	Record(record *MessageRecord) error
	Update(correlationID string, updates map[string]interface{}) error
	GetByID(id uint) (*MessageRecord, error)
	GetByCorrelationID(correlationID string) (*MessageRecord, error)
	List(query *MessageQuery) ([]MessageRecord, int64, error)
	MarkSuccess(correlationID string, response []byte, processingTime int64) error
	MarkFailed(correlationID string, errMsg string) error
	MarkTimeout(correlationID string) error
}

// MessageQuery 消息查询条件
type MessageQuery struct {
	CorrelationID string
	Type          MessageType
	SubType       string
	Direction     string
	Status        string
	Source        string
	StartTime     *time.Time
	EndTime       *time.Time
	Page          int
	PageSize      int
}

// defaultMessageRecorder 默认消息记录器实现
type defaultMessageRecorder struct {
	db *gorm.DB
	mu sync.RWMutex
}

// NewMessageRecorder 创建消息记录器
// 参数:
//   - db: GORM数据库实例
//
// 返回值:
//   - MessageRecorder: 消息记录器实例
func NewMessageRecorder(db *gorm.DB) MessageRecorder {
	return &defaultMessageRecorder{db: db}
}

// NewMessageRecorderWithConfig 使用配置创建消息记录器
// 参数:
//   - config: 数据库配置
//
// 返回值:
//   - MessageRecorder: 消息记录器实例
//   - error: 创建失败时返回错误
func NewMessageRecorderWithConfig(config DatabaseConfig) (MessageRecorder, error) {
	db, err := connectDatabase(config)
	if err != nil {
		return nil, err
	}

	if err := autoMigrate(db); err != nil {
		return nil, err
	}

	return &defaultMessageRecorder{db: db}, nil
}

// connectDatabase 连接MySQL数据库
func connectDatabase(config DatabaseConfig) (*gorm.DB, error) {
	logLevel := logger.Silent
	switch config.LogLevel {
	case "error":
		logLevel = logger.Error
	case "warn":
		logLevel = logger.Warn
	case "info":
		logLevel = logger.Info
	}

	db, err := gorm.Open(mysql.Open(config.DSN), &gorm.Config{
		Logger: logger.Default.LogMode(logLevel),
	})
	if err != nil {
		return nil, fmt.Errorf("连接MySQL数据库失败: %w", err)
	}

	return db, nil
}

// autoMigrate 自动迁移数据库表结构
func autoMigrate(db *gorm.DB) error {
	return db.AutoMigrate(&MessageRecord{})
}

// Record 记录消息
func (r *defaultMessageRecorder) Record(record *MessageRecord) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if record.CreatedAt.IsZero() {
		record.CreatedAt = time.Now()
	}
	record.UpdatedAt = time.Now()

	return r.db.Create(record).Error
}

// Update 更新消息记录
func (r *defaultMessageRecorder) Update(correlationID string, updates map[string]interface{}) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	updates["updated_at"] = time.Now()
	return r.db.Model(&MessageRecord{}).
		Where("correlation_id = ?", correlationID).
		Updates(updates).Error
}

// GetByID 根据ID获取消息记录
func (r *defaultMessageRecorder) GetByID(id uint) (*MessageRecord, error) {
	var record MessageRecord
	err := r.db.First(&record, id).Error
	if err != nil {
		return nil, err
	}
	return &record, nil
}

// GetByCorrelationID 根据关联ID获取消息记录
func (r *defaultMessageRecorder) GetByCorrelationID(correlationID string) (*MessageRecord, error) {
	var record MessageRecord
	err := r.db.Where("correlation_id = ?", correlationID).First(&record).Error
	if err != nil {
		return nil, err
	}
	return &record, nil
}

// List 查询消息记录列表
func (r *defaultMessageRecorder) List(query *MessageQuery) ([]MessageRecord, int64, error) {
	var records []MessageRecord
	var total int64

	db := r.db.Model(&MessageRecord{})

	if query.CorrelationID != "" {
		db = db.Where("correlation_id = ?", query.CorrelationID)
	}
	if query.Type != "" {
		db = db.Where("type = ?", query.Type)
	}
	if query.SubType != "" {
		db = db.Where("sub_type = ?", query.SubType)
	}
	if query.Direction != "" {
		db = db.Where("direction = ?", query.Direction)
	}
	if query.Status != "" {
		db = db.Where("status = ?", query.Status)
	}
	if query.Source != "" {
		db = db.Where("source = ?", query.Source)
	}
	if query.StartTime != nil {
		db = db.Where("created_at >= ?", query.StartTime)
	}
	if query.EndTime != nil {
		db = db.Where("created_at <= ?", query.EndTime)
	}

	if err := db.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	page := query.Page
	if page < 1 {
		page = 1
	}
	pageSize := query.PageSize
	if pageSize < 1 {
		pageSize = 20
	}
	if pageSize > 100 {
		pageSize = 100
	}

	offset := (page - 1) * pageSize
	err := db.Order("created_at DESC").Offset(offset).Limit(pageSize).Find(&records).Error
	if err != nil {
		return nil, 0, err
	}

	return records, total, nil
}

// MarkSuccess 标记消息成功
func (r *defaultMessageRecorder) MarkSuccess(correlationID string, response []byte, processingTime int64) error {
	now := time.Now()
	return r.Update(correlationID, map[string]interface{}{
		"status":          StatusSuccess,
		"response":        response,
		"processing_time": processingTime,
		"completed_at":    &now,
	})
}

// MarkFailed 标记消息失败
func (r *defaultMessageRecorder) MarkFailed(correlationID string, errMsg string) error {
	now := time.Now()
	return r.Update(correlationID, map[string]interface{}{
		"status":       StatusFailed,
		"error_msg":    errMsg,
		"completed_at": &now,
	})
}

// MarkTimeout 标记消息超时
func (r *defaultMessageRecorder) MarkTimeout(correlationID string) error {
	now := time.Now()
	return r.Update(correlationID, map[string]interface{}{
		"status":       StatusTimeout,
		"error_msg":    "请求超时",
		"completed_at": &now,
	})
}

// noopMessageRecorder 空实现（不记录）
type noopMessageRecorder struct{}

func (r *noopMessageRecorder) Record(record *MessageRecord) error {
	return nil
}

func (r *noopMessageRecorder) Update(correlationID string, updates map[string]interface{}) error {
	return nil
}

func (r *noopMessageRecorder) GetByID(id uint) (*MessageRecord, error) {
	return nil, nil
}

func (r *noopMessageRecorder) GetByCorrelationID(correlationID string) (*MessageRecord, error) {
	return nil, nil
}

func (r *noopMessageRecorder) List(query *MessageQuery) ([]MessageRecord, int64, error) {
	return nil, 0, nil
}

func (r *noopMessageRecorder) MarkSuccess(correlationID string, response []byte, processingTime int64) error {
	return nil
}

func (r *noopMessageRecorder) MarkFailed(correlationID string, errMsg string) error {
	return nil
}

func (r *noopMessageRecorder) MarkTimeout(correlationID string) error {
	return nil
}

// NoopMessageRecorder 返回一个空的消息记录器
func NoopMessageRecorder() MessageRecorder {
	return &noopMessageRecorder{}
}
