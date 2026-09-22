// Package pipeline 提供一条纯本地、可恢复的异步事件投递与消费链路。
//
// 投递语义：at-least-once（至少一次），业务效果 exactly-once（由幂等存储保证）。
package pipeline

import "time"

// Event 是流经管道的不可变事件。
type Event struct {
	ID        string    // 事件全局唯一标识（生产者负责唯一），幂等判定依据
	Key       string    // 业务键：同 Key 事件严格按日志顺序处理，不同 Key 可并行
	Type      string    // 事件类型，由业务 Handler 解释
	Payload   []byte    // 业务负载
	CreatedAt time.Time // 事件产生时间
}

// StoredEvent 是事件在本地日志中的落地形态。
type StoredEvent struct {
	Offset   int64 // 单调递增的日志位置（从 0 开始）
	Event    Event
	StoredAt time.Time
}
