package main

import (
	"context"
	"fmt"
	"log"
	"time"

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
		WithTimeout(5 * time.Second)

	// 创建客户端
	client, err := rpc.NewClient(config)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	fmt.Println("[Client] 已连接到 Pulsar")

	// 示例1: 发送计算请求（带子类型）
	calculateExample(client)

	// 示例2: 发送回显请求（带子类型）
	echoExample(client)

	// 示例3: 批量异步调用
	asyncExample(client)
}

// calculateExample 计算请求示例
func calculateExample(client *rpc.Client) {
	fmt.Println("\n[Client] === 计算请求示例 ===")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 创建带子类型的消息
	msg, err := rpc.NewMessageWithType(rpc.MessageTypeRequest, SubTypeCalculate, &CalculateRequest{
		A: 10,
		B: 20,
	})
	if err != nil {
		log.Printf("[Client] 创建消息失败: %v\n", err)
		return
	}

	fmt.Printf("[Client] 发送计算请求: A=10, B=20, SubType=%s\n", msg.SubType)

	// 发送请求
	resp, err := client.Call(ctx, msg)
	if err != nil {
		log.Printf("[Client] 调用失败: %v\n", err)
		return
	}

	// 检查响应类型
	if resp.IsError() {
		log.Printf("[Client] 收到错误响应: %s\n", resp.Error)
		return
	}

	// 根据子类型解析响应
	var result CalculateResponse
	if err := resp.ParsePayloadBySubType(SubTypeCalculate, &result); err != nil {
		log.Printf("[Client] 解析响应失败: %v\n", err)
		return
	}

	fmt.Printf("[Client] 计算结果: 10 + 20 = %d\n", result.Result)
}

// echoExample 回显请求示例
func echoExample(client *rpc.Client) {
	fmt.Println("\n[Client] === 回显请求示例 ===")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 创建带子类型的消息
	msg, err := rpc.NewMessageWithType(rpc.MessageTypeRequest, SubTypeEcho, &EchoRequest{
		Message: "Hello Pulsar RPC!",
	})
	if err != nil {
		log.Printf("[Client] 创建消息失败: %v\n", err)
		return
	}

	fmt.Printf("[Client] 发送回显请求: Hello Pulsar RPC!, SubType=%s\n", msg.SubType)

	// 发送请求
	resp, err := client.Call(ctx, msg)
	if err != nil {
		log.Printf("[Client] 调用失败: %v\n", err)
		return
	}

	// 检查响应类型
	if resp.IsError() {
		log.Printf("[Client] 收到错误响应: %s\n", resp.Error)
		return
	}

	// 解析响应
	var result EchoResponse
	if err := resp.ParsePayload(&result); err != nil {
		log.Printf("[Client] 解析响应失败: %v\n", err)
		return
	}

	fmt.Printf("[Client] 回显结果: %s\n", result.Message)
}

// asyncExample 异步批量调用示例
func asyncExample(client *rpc.Client) {
	fmt.Println("\n[Client] === 异步批量调用示例 ===")

	ctx := context.Background()

	// 创建多个不同类型的请求
	requests := []struct {
		subType string
		msg     *rpc.Message
	}{
		{
			subType: SubTypeCalculate,
		},
		{
			subType: SubTypeEcho,
		},
	}

	// 创建计算请求
	calcMsg, _ := rpc.NewMessageWithType(rpc.MessageTypeRequest, SubTypeCalculate, &CalculateRequest{A: 100, B: 200})
	requests[0].msg = calcMsg

	// 创建回显请求
	echoMsg, _ := rpc.NewMessageWithType(rpc.MessageTypeRequest, SubTypeEcho, &EchoRequest{Message: "Async Hello!"})
	requests[1].msg = echoMsg

	// 发起所有异步请求
	results := make([]<-chan *rpc.CallResult, len(requests))
	for i, req := range requests {
		results[i] = client.CallAsync(ctx, req.msg)
		fmt.Printf("[Client] 发起异步请求 %d: SubType=%s\n", i, req.subType)
	}

	// 收集所有结果
	fmt.Println("[Client] 等待异步结果...")
	for i, ch := range results {
		result := <-ch
		if result.Error != nil {
			fmt.Printf("[Client] 请求 %d 失败: %v\n", i, result.Error)
			continue
		}

		if result.Response.IsError() {
			fmt.Printf("[Client] 请求 %d 返回错误: %s\n", i, result.Response.Error)
			continue
		}

		// 根据子类型解析响应
		switch requests[i].subType {
		case SubTypeCalculate:
			var resp CalculateResponse
			if err := result.Response.ParsePayload(&resp); err != nil {
				fmt.Printf("[Client] 请求 %d 解析失败: %v\n", i, err)
				continue
			}
			fmt.Printf("[Client] 请求 %d 计算结果: %d\n", i, resp.Result)

		case SubTypeEcho:
			var resp EchoResponse
			if err := result.Response.ParsePayload(&resp); err != nil {
				fmt.Printf("[Client] 请求 %d 解析失败: %v\n", i, err)
				continue
			}
			fmt.Printf("[Client] 请求 %d 回显结果: %s\n", i, resp.Message)
		}
	}
}
