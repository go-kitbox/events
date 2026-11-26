package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	rpc "github.com/go-kitbox/events/mq_event"
)

// 定义消息子类型常量
const (
	SubTypeCalculate = "calculate"
	SubTypeEcho      = "echo"
)

// CalculateRequest 计算请求结构体
type CalculateRequest struct {
	A int `json:"a"`
	B int `json:"b"`
}

// CalculateResponse 计算响应结构体
type CalculateResponse struct {
	Result int `json:"result"`
}

// EchoRequest 回显请求结构体
type EchoRequest struct {
	Message string `json:"message"`
}

// EchoResponse 回显响应结构体
type EchoResponse struct {
	Message string `json:"message"`
}

func main() {
	// 创建配置
	config := rpc.DefaultConfig().
		WithURL("pulsar://localhost:6650").
		WithTopic("task-topic").
		WithReplyTopic("reply-topic").
		WithWorkerCount(10)

	// 创建子类型路由器
	router := rpc.NewSubTypeRouter()

	// 注册 calculate 子类型处理器
	router.Register(SubTypeCalculate, func(payload json.RawMessage) (interface{}, error) {
		var req CalculateRequest
		if err := json.Unmarshal(payload, &req); err != nil {
			return nil, err
		}
		fmt.Printf("[Worker] 收到计算请求: A=%d, B=%d\n", req.A, req.B)
		return &CalculateResponse{
			Result: req.A + req.B,
		}, nil
	})

	// 注册 echo 子类型处理器
	router.Register(SubTypeEcho, func(payload json.RawMessage) (interface{}, error) {
		var req EchoRequest
		if err := json.Unmarshal(payload, &req); err != nil {
			return nil, err
		}
		fmt.Printf("[Worker] 收到回显请求: %s\n", req.Message)
		return &EchoResponse{
			Message: req.Message,
		}, nil
	})

	// 创建消息处理函数
	handler := func(ctx context.Context, msg *rpc.Message) (interface{}, error) {
		fmt.Printf("[Worker] 收到消息: Type=%s, SubType=%s\n", msg.Type, msg.SubType)

		// 根据子类型路由到不同处理器
		return router.Handle(msg)
	}

	// 创建Worker
	worker, err := rpc.NewWorker(config, handler)
	if err != nil {
		log.Fatal(err)
	}

	// 启动Worker
	worker.Start()
	fmt.Println("[Worker] 已启动，按 Ctrl+C 停止...")

	// 等待中断信号
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	// 优雅关闭
	fmt.Println("[Worker] 正在停止...")
	if err := worker.Stop(); err != nil {
		log.Printf("[Worker] 停止失败: %v\n", err)
	}
	fmt.Println("[Worker] 已停止")
}
