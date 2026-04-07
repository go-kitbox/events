package pulsarrpc

import (
	"time"
)

// CompressionType 压缩类型枚举
type CompressionType int

const (
	CompressionNone CompressionType = iota
	CompressionLZ4
	CompressionZLib
	CompressionZSTD
)

// Config RPC框架配置结构体
// 包含Pulsar连接、超时、重试等所有配置项
type Config struct {
	Pulsar   PulsarConfig   `json:"pulsar" yaml:"pulsar"`     // Pulsar连接配置
	RPC      RPCConfig      `json:"rpc" yaml:"rpc"`           // RPC行为配置
	Database DatabaseConfig `json:"database" yaml:"database"` // 数据库配置
	Recorder RecorderConfig `json:"recorder" yaml:"recorder"` // 消息记录配置
}

// RecorderConfig 消息记录配置
type RecorderConfig struct {
	Enabled bool   `json:"enabled" yaml:"enabled"` // 是否启用消息记录
	Source  string `json:"source" yaml:"source"`   // 来源标识（用于区分不同服务）
	Async   bool   `json:"async" yaml:"async"`     // 是否异步记录
}

// PulsarConfig Pulsar连接配置
type PulsarConfig struct {
	URL string `json:"url" yaml:"url"` // Pulsar broker地址，例如: pulsar://localhost:6650

	Topic        string `json:"topic" yaml:"topic"`               // 请求主题
	ReplyTopic   string `json:"reply_topic" yaml:"reply_topic"`   // 响应主题
	Subscription string `json:"subscription" yaml:"subscription"` // 订阅名称

	Auth *AuthConfig `json:"auth,omitempty" yaml:"auth,omitempty"` // 认证配置（可选）
	TLS  *TLSConfig  `json:"tls,omitempty" yaml:"tls,omitempty"`   // TLS配置（可选）

	Compression  CompressionType `json:"compression" yaml:"compression"`       // 压缩类型
	Batching     bool            `json:"batching" yaml:"batching"`             // 是否启用批量发送
	BatchMaxSize uint            `json:"batch_max_size" yaml:"batch_max_size"` // 批量最大消息数
}

// AuthConfig 认证配置
type AuthConfig struct {
	Type string `json:"type" yaml:"type"` // 认证类型: token, oauth2, athenz

	Token string `json:"token,omitempty" yaml:"token,omitempty"` // Token认证的令牌

	OAuth2 *OAuth2Config `json:"oauth2,omitempty" yaml:"oauth2,omitempty"` // OAuth2配置
}

// OAuth2Config OAuth2认证配置
type OAuth2Config struct {
	IssuerURL  string `json:"issuer_url" yaml:"issuer_url"`
	ClientID   string `json:"client_id" yaml:"client_id"`
	Audience   string `json:"audience" yaml:"audience"`
	PrivateKey string `json:"private_key" yaml:"private_key"`
}

// TLSConfig TLS安全配置
type TLSConfig struct {
	Enabled        bool   `json:"enabled" yaml:"enabled"`
	TrustCertsFile string `json:"trust_certs_file" yaml:"trust_certs_file"`
	AllowInsecure  bool   `json:"allow_insecure" yaml:"allow_insecure"`
}

// RPCConfig RPC行为配置
type RPCConfig struct {
	Timeout    time.Duration `json:"timeout" yaml:"timeout"`         // 单次请求超时时间
	MaxRetries int           `json:"max_retries" yaml:"max_retries"` // 最大重试次数
	RetryDelay time.Duration `json:"retry_delay" yaml:"retry_delay"` // 重试间隔

	WorkerCount int `json:"worker_count" yaml:"worker_count"` // Worker并发数
	QueueSize   int `json:"queue_size" yaml:"queue_size"`     // 内部队列大小

	Concurrency int `json:"concurrency" yaml:"concurrency"` // 最大并发请求数
}

// DefaultConfig 返回默认配置
// 返回值:
//   - *Config: 包含合理默认值的配置实例
func DefaultConfig() *Config {
	return &Config{
		Pulsar: PulsarConfig{
			URL:          "pulsar://localhost:6650",
			Topic:        "task-topic",
			ReplyTopic:   "reply-topic",
			Subscription: "rpc-subscription",
			Compression:  CompressionLZ4,
			Batching:     true,
			BatchMaxSize: 1000,
		},
		RPC: RPCConfig{
			Timeout:     10 * time.Second,
			MaxRetries:  3,
			RetryDelay:  100 * time.Millisecond,
			WorkerCount: 10,
			QueueSize:   10000,
			Concurrency: 1000,
		},
	}
}

// Validate 验证配置有效性
// 返回值:
//   - error: 配置无效时返回错误
func (c *Config) Validate() error {
	if c.Pulsar.URL == "" {
		return ErrInvalidConfig{Field: "Pulsar.URL", Message: "Pulsar URL不能为空"}
	}
	if c.Pulsar.Topic == "" {
		return ErrInvalidConfig{Field: "Pulsar.Topic", Message: "Topic不能为空"}
	}
	if c.RPC.Timeout <= 0 {
		return ErrInvalidConfig{Field: "RPC.Timeout", Message: "超时时间必须大于0"}
	}
	if c.RPC.MaxRetries < 0 {
		return ErrInvalidConfig{Field: "RPC.MaxRetries", Message: "重试次数不能为负数"}
	}
	if c.RPC.WorkerCount <= 0 {
		return ErrInvalidConfig{Field: "RPC.WorkerCount", Message: "Worker数量必须大于0"}
	}
	return nil
}

// ErrInvalidConfig 无效配置错误
type ErrInvalidConfig struct {
	Field   string
	Message string
}

func (e ErrInvalidConfig) Error() string {
	return "配置错误 [" + e.Field + "]: " + e.Message
}

// WithURL 设置Pulsar URL（链式调用）
func (c *Config) WithURL(url string) *Config {
	c.Pulsar.URL = url
	return c
}

// WithTopic 设置请求主题（链式调用）
func (c *Config) WithTopic(topic string) *Config {
	c.Pulsar.Topic = topic
	return c
}

// WithReplyTopic 设置响应主题（链式调用）
func (c *Config) WithReplyTopic(topic string) *Config {
	c.Pulsar.ReplyTopic = topic
	return c
}

// WithTimeout 设置超时时间（链式调用）
func (c *Config) WithTimeout(timeout time.Duration) *Config {
	c.RPC.Timeout = timeout
	return c
}

// WithMaxRetries 设置最大重试次数（链式调用）
func (c *Config) WithMaxRetries(maxRetries int) *Config {
	c.RPC.MaxRetries = maxRetries
	return c
}

// WithWorkerCount 设置Worker数量（链式调用）
func (c *Config) WithWorkerCount(count int) *Config {
	c.RPC.WorkerCount = count
	return c
}

// WithCompression 设置压缩类型（链式调用）
func (c *Config) WithCompression(compression CompressionType) *Config {
	c.Pulsar.Compression = compression
	return c
}

// WithTokenAuth 设置Token认证（链式调用）
func (c *Config) WithTokenAuth(token string) *Config {
	c.Pulsar.Auth = &AuthConfig{
		Type:  "token",
		Token: token,
	}
	return c
}

// WithDatabase 设置数据库配置（链式调用）
func (c *Config) WithDatabase(driver, dsn string) *Config {
	c.Database = DatabaseConfig{
		Driver: driver,
		DSN:    dsn,
	}
	return c
}

// WithRecorder 设置消息记录配置（链式调用）
func (c *Config) WithRecorder(enabled bool, source string) *Config {
	c.Recorder = RecorderConfig{
		Enabled: enabled,
		Source:  source,
	}
	return c
}

// WithAsyncRecorder 设置异步消息记录（链式调用）
func (c *Config) WithAsyncRecorder(async bool) *Config {
	c.Recorder.Async = async
	return c
}
