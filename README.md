# Pulsar RPC

基于 Apache Pulsar 的高性能 Go RPC 框架，支持任务调度、超时控制、自动重试和水平扩展。

## 特性

- ✅ 支持同步/异步 RPC 调用
- ✅ Channel 风格返回值
- ✅ 超时控制
- ✅ 自动重试机制
- ✅ 多 Worker 水平扩展
- ✅ 消息压缩（LZ4/ZLib/ZSTD）
- ✅ 批量发送优化
- ✅ Token 认证支持

## 安装

```bash
go get github.com/osc678/pulsar-rpc
```

## 快速开始

### 1. 配置文件 (config.yaml)

```yaml
pulsar:
  url: "pulsar://localhost:6650"
  topic: "task-topic"
  reply_topic: "reply-topic"
  subscription: "rpc-subscription"
  compression: 1        # 0=none, 1=lz4, 2=zlib, 3=zstd
  batching: true
  batch_max_size: 1000

rpc:
  timeout: "10s"
  max_retries: 3
  retry_delay: "100ms"
  worker_count: 10
  queue_size: 10000
  concurrency: 1000
```

### 2. 服务端 (Worker)

```go
package main

import (
    "context"
    "encoding/json"
    "fmt"
    "log"
    
    rpc "github.com/osc678/pulsar-rpc/pulsar_rpc"
)

type CalculateRequest struct {
    A int `json:"a"`
    B int `json:"b"`
}

type CalculateResponse struct {
    Result int `json:"result"`
}

func main() {
    config := rpc.DefaultConfig().
        WithURL("pulsar://localhost:6650").
        WithTopic("task-topic").
        WithReplyTopic("reply-topic").
        WithWorkerCount(10)

    handler := func(ctx context.Context, msg *rpc.Message) (interface{}, error) {
        var req CalculateRequest
        if err := msg.ParsePayload(&req); err != nil {
            return nil, err
        }
        
        return &CalculateResponse{
            Result: req.A + req.B,
        }, nil
    }

    worker, err := rpc.NewWorker(config, handler)
    if err != nil {
        log.Fatal(err)
    }

    worker.Start()
    fmt.Println("Worker 已启动，按 Ctrl+C 停止...")

    select {}
}
```

### 3. 客户端 (Client)

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"
    
    rpc "github.com/osc678/pulsar-rpc/pulsar_rpc"
)

type CalculateRequest struct {
    A int `json:"a"`
    B int `json:"b"`
}

type CalculateResponse struct {
    Result int `json:"result"`
}

func main() {
    config := rpc.DefaultConfig().
        WithURL("pulsar://localhost:6650").
        WithTopic("task-topic").
        WithReplyTopic("reply-topic").
        WithTimeout(5 * time.Second)

    client, err := rpc.NewClient(config)
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    // 同步调用
    syncCall(client)

    // 异步调用 (channel 风格)
    asyncCall(client)
}

func syncCall(client *rpc.Client) {
    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()

    resp, err := client.Call(ctx, &CalculateRequest{A: 10, B: 20})
    if err != nil {
        log.Printf("调用失败: %v", err)
        return
    }

    var result CalculateResponse
    if err := resp.ParsePayload(&result); err != nil {
        log.Printf("解析响应失败: %v", err)
        return
    }

    fmt.Printf("同步调用结果: %d\n", result.Result)
}

func asyncCall(client *rpc.Client) {
    ctx := context.Background()

    // 批量并发调用
    requests := []CalculateRequest{
        {A: 1, B: 2},
        {A: 3, B: 4},
        {A: 5, B: 6},
    }

    results := make([]<-chan *rpc.CallResult, len(requests))
    for i, req := range requests {
        results[i] = client.CallAsync(ctx, req)
    }

    for i, ch := range results {
        result := <-ch
        if result.Error != nil {
            fmt.Printf("请求 %d 失败: %v\n", i, result.Error)
            continue
        }

        var resp CalculateResponse
        if err := result.Response.ParsePayload(&resp); err != nil {
            fmt.Printf("请求 %d 解析失败: %v\n", i, err)
            continue
        }

        fmt.Printf("请求 %d 结果: %d\n", i, resp.Result)
    }
}
```

## 配置说明

### PulsarConfig

| 字段 | 类型 | 说明 | 默认值 |
|------|------|------|--------|
| `url` | string | Pulsar broker 地址 | `pulsar://localhost:6650` |
| `topic` | string | 请求主题 | `task-topic` |
| `reply_topic` | string | 响应主题 | `reply-topic` |
| `subscription` | string | 订阅名称 | `rpc-subscription` |
| `compression` | int | 压缩类型 (0=none, 1=lz4, 2=zlib, 3=zstd) | 1 (LZ4) |
| `batching` | bool | 是否启用批量发送 | true |
| `batch_max_size` | uint | 批量最大消息数 | 1000 |

### RPCConfig

| 字段 | 类型 | 说明 | 默认值 |
|------|------|------|--------|
| `timeout` | duration | 单次请求超时 | 10s |
| `max_retries` | int | 最大重试次数 | 3 |
| `retry_delay` | duration | 重试间隔 | 100ms |
| `worker_count` | int | Worker 并发数 | 10 |
| `queue_size` | int | 内部队列大小 | 10000 |
| `concurrency` | int | 最大并发请求数 | 1000 |

## API 参考

### Client

```go
// 同步调用
func (c *Client) Call(ctx context.Context, payload interface{}) (*Message, error)

// 异步调用 (返回 channel)
func (c *Client) CallAsync(ctx context.Context, payload interface{}) <-chan *CallResult

// 关闭客户端
func (c *Client) Close() error
```

### Worker

```go
// 创建 Worker
func NewWorker(config *Config, handler Handler) (*Worker, error)

// 启动
func (w *Worker) Start()

// 停止
func (w *Worker) Stop() error
```

### Message

```go
// 创建消息
func NewMessage(payload interface{}) (*Message, error)

// 解析负载
func (m *Message) ParsePayload(target interface{}) error

// 创建响应
func CreateResponse(correlationID string, payload interface{}) (*Message, error)
```

## 生产建议

| 配置项 | 建议值 |
|--------|--------|
| Topic 分区数 | 8~32 |
| Worker 数量 | 每核 1~2 个 goroutine |
| 压缩 | LZ4 |
| 批量发送 | 开启 |
| 超时 | 3~10 秒 |
| 重试次数 | 2~3 次 |

## 性能预估

- **单机吞吐**: 5~10 万 QPS
- **延迟**: < 5ms (内网)
- **扩展**: 多节点水平扩展

## 依赖

- Go 1.21+
- Apache Pulsar 2.10+
- github.com/apache/pulsar-client-go

## License

MIT
