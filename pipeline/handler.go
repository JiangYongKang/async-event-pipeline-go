package pipeline

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// Handler 承载业务逻辑：在传入的（可丢弃的）业务状态副本上施加事件效果。
//
// 返回 nil 表示处理成功；返回 *EventError（FailurePermanent/FailureInvalid）
// 直接进入死信；返回其他错误或 FailureTransient 按退避重试，
// 超过 Config.MaxAttempts 后以 FailureTransient 进入死信。
//
// 实现方必须保证 Handle 对 state 的修改只在成功返回时才会被持久化：
// 运行时在克隆上调用 Handle，失败即丢弃克隆，因此不会留下半完成状态。
type Handler interface {
	Handle(ctx context.Context, ev StoredEvent, state BusinessState) error
}

// HandlerFunc 让普通函数满足 Handler。
type HandlerFunc func(ctx context.Context, ev StoredEvent, state BusinessState) error

func (f HandlerFunc) Handle(ctx context.Context, ev StoredEvent, state BusinessState) error {
	return f(ctx, ev, state)
}

// CountHandler 是内置示例 Handler：解释事件并修改 *KVState。
//
//	Type="inc" Payload=<key>          该 key 计数 +1（重复投递不重复累加）
//	Type="add" Payload="<key>=<n>"    该 key 计数 +n（n 可为负数）
//
// Payload 无法解析时返回 FailureInvalid（毒丸，不重试）。
type CountHandler struct {
	// FailEventIDs：事件ID -> 仍需失败的次数（瞬态错误）。每次失败原子递减，
	// 降到 0 后正常放行。用于测试重试链路；多 worker 并发下也是安全的。
	failMu    sync.Mutex
	fail      map[string]*int64
	permanent map[string]string
}

// NewCountHandler 创建 CountHandler。
func NewCountHandler() *CountHandler {
	return &CountHandler{
		fail:      make(map[string]*int64),
		permanent: make(map[string]string),
	}
}

// FailNTimes 让事件 id 接下来前 n 次 Handle 返回瞬态错误，之后成功。
func (h *CountHandler) FailNTimes(id string, n int) *CountHandler {
	h.failMu.Lock()
	defer h.failMu.Unlock()
	v := int64(n)
	h.fail[id] = &v
	return h
}

// FailPermanent 让事件 id 第一次处理即返回不可重试错误（直接死信）。
func (h *CountHandler) FailPermanent(id, reason string) *CountHandler {
	h.failMu.Lock()
	defer h.failMu.Unlock()
	h.fail[id] = nil // nil 哨兵表示永久失败
	h.permanent[id] = reason
	return h
}

func (h *CountHandler) Handle(ctx context.Context, ev StoredEvent, state BusinessState) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// 注入失败（重试链路测试用）。
	h.failMu.Lock()
	counter, hasFail := h.fail[ev.Event.ID]
	if hasFail && counter == nil {
		reason := h.permanent[ev.Event.ID]
		h.failMu.Unlock()
		return PermanentError(reason)
	}
	if hasFail && atomic.LoadInt64(counter) > 0 {
		atomic.AddInt64(counter, -1)
		h.failMu.Unlock()
		return TransientError("injected transient failure")
	}
	if hasFail {
		delete(h.fail, ev.Event.ID)
		delete(h.permanent, ev.Event.ID)
	}
	h.failMu.Unlock()

	kv, ok := state.(*KVState)
	if !ok {
		return InvalidError("CountHandler requires *pipeline.KVState business state")
	}
	if kv.Values == nil {
		kv.Values = make(map[string]int64)
	}
	switch ev.Event.Type {
	case "inc":
		key := string(ev.Event.Payload)
		if key == "" {
			return InvalidError(`"inc" event requires non-empty payload key`)
		}
		kv.Values[key]++
	case "add":
		key, n, err := parseAdd(ev.Event.Payload)
		if err != nil {
			return InvalidError(err.Error())
		}
		kv.Values[key] += n
	default:
		return PermanentError("unknown event type: " + ev.Event.Type)
	}
	return nil
}

func parseAdd(payload []byte) (string, int64, error) {
	parts := strings.SplitN(string(payload), "=", 2)
	if len(parts) != 2 || parts[0] == "" {
		return "", 0, errBadAddPayload
	}
	n, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return "", 0, errBadAddPayload
	}
	return parts[0], n, nil
}

var errBadAddPayload = &simpleError{msg: `invalid "add" payload, want "<key>=<int64>"`}

type simpleError struct{ msg string }

func (e *simpleError) Error() string { return e.msg }
