package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	rpc "github.com/go-kitbox/events/mq_event"
)

// UserCalcPayload 用户计算事件负载
type UserCalcPayload struct {
	UID    int64   `json:"uid"`
	Action string  `json:"action"`
	Amount float64 `json:"amount"`
}

// UserNotifyPayload 用户通知事件负载
type UserNotifyPayload struct {
	UID     int64  `json:"uid"`
	Title   string `json:"title"`
	Content string `json:"content"`
}

func main() {
	// 创建配置
	config := rpc.DefaultConfig().
		WithURL("pulsar://localhost:6650")

	// 创建事件总线
	eventBus, err := rpc.NewEventBus(config, "event-topic", "event-subscription")
	if err != nil {
		log.Fatal(err)
	}

	// 注册事件处理器
	registerHandlers(eventBus)

	// 启动事件总线
	eventBus.Start()
	defer eventBus.Stop()

	fmt.Printf("[EventNode] 节点 %s 已启动，等待事件...\n", eventBus.NodeID())

	// 模拟发布事件
	go publishEvents(eventBus)

	// 等待中断信号
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println("[EventNode] 正在停止...")
}

// registerHandlers 注册事件处理器
func registerHandlers(bus *rpc.EventBus) {
	// 处理用户计算事件
	bus.Subscribe(rpc.EventTypeUserCalc, func(ctx context.Context, event *rpc.Event) error {
		var payload UserCalcPayload
		if err := event.ParsePayload(&payload); err != nil {
			return err
		}

		fmt.Printf("[EventNode] 收到用户计算事件: UID=%d, Action=%s, Amount=%.2f, Source=%s\n",
			payload.UID, payload.Action, payload.Amount, event.Source)

		// 这里可以执行具体的业务逻辑
		return nil
	})

	// 处理用户通知事件
	bus.Subscribe(rpc.EventTypeUserNotify, func(ctx context.Context, event *rpc.Event) error {
		var payload UserNotifyPayload
		if err := event.ParsePayload(&payload); err != nil {
			return err
		}

		fmt.Printf("[EventNode] 收到用户通知事件: UID=%d, Title=%s, Source=%s\n",
			payload.UID, payload.Title, event.Source)

		return nil
	})

	// 处理系统任务事件
	bus.Subscribe(rpc.EventTypeSystemTask, func(ctx context.Context, event *rpc.Event) error {
		fmt.Printf("[EventNode] 收到系统任务事件: SubType=%s, EventID=%s\n",
			event.SubType, event.EventID)
		return nil
	})

	// 处理广播事件
	bus.Subscribe(rpc.EventTypeBroadcast, func(ctx context.Context, event *rpc.Event) error {
		fmt.Printf("[EventNode] 收到广播事件: Source=%s, Payload=%s\n",
			event.Source, string(event.Payload))
		return nil
	})
}

// publishEvents 模拟发布事件
func publishEvents(bus *rpc.EventBus) {
	time.Sleep(2 * time.Second)

	ctx := context.Background()

	// 发布用户计算事件
	calcEvent, _ := rpc.NewEvent(rpc.EventTypeUserCalc, &UserCalcPayload{
		UID:    1001,
		Action: "bet",
		Amount: 100.00,
	})
	calcEvent.WithUID(1001).WithMetadata("trace_id", "trace-001")
	if err := bus.Emit(ctx, calcEvent); err != nil {
		log.Printf("发布用户计算事件失败: %v\n", err)
	} else {
		fmt.Printf("[EventNode] 已发布用户计算事件: EventID=%s\n", calcEvent.EventID)
	}

	time.Sleep(1 * time.Second)

	// 发布用户通知事件
	notifyEvent, _ := rpc.NewEvent(rpc.EventTypeUserNotify, &UserNotifyPayload{
		UID:     1001,
		Title:   "中奖通知",
		Content: "恭喜您中奖了！",
	})
	notifyEvent.WithUID(1001)
	if err := bus.Emit(ctx, notifyEvent); err != nil {
		log.Printf("发布用户通知事件失败: %v\n", err)
	} else {
		fmt.Printf("[EventNode] 已发布用户通知事件: EventID=%s\n", notifyEvent.EventID)
	}

	time.Sleep(1 * time.Second)

	// 发布系统任务事件（UID=0表示系统）
	taskEvent, _ := rpc.NewEventWithSubType(rpc.EventTypeSystemTask, "cleanup", json.RawMessage(`{"action": "clean_logs"}`))
	taskEvent.WithUID(0)
	if err := bus.Emit(ctx, taskEvent); err != nil {
		log.Printf("发布系统任务事件失败: %v\n", err)
	} else {
		fmt.Printf("[EventNode] 已发布系统任务事件: EventID=%s\n", taskEvent.EventID)
	}

	time.Sleep(1 * time.Second)

	// 发布广播事件
	broadcastEvent, _ := rpc.NewEvent(rpc.EventTypeBroadcast, map[string]string{
		"message": "系统维护通知",
		"time":    "10分钟后",
	})
	if err := bus.Emit(ctx, broadcastEvent); err != nil {
		log.Printf("发布广播事件失败: %v\n", err)
	} else {
		fmt.Printf("[EventNode] 已发布广播事件: EventID=%s\n", broadcastEvent.EventID)
	}
}
