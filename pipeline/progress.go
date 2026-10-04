package pipeline

import (
	"context"
	"fmt"
	"sort"
	"time"
)

// gapSampleLimit 限制 ProgressReport.Gaps 中逐条列出的空洞数量，
// 防止报告随积压无界增长；未列出的部分仍计入各聚合计数。
const gapSampleLimit = 30

// jobPhase 描述一条已派发但尚未到达终态的事件当前所处的阶段，
// 仅用于运行期可观测与完成判定，不参与持久化。
type jobPhase string

const (
	phaseQueued     jobPhase = "queued"     // 已入键队列，等待同键前序或并行许可
	phaseProcessing jobPhase = "processing" // Handler 正在执行
	phaseRetrying   jobPhase = "retrying"   // 瞬态失败后正在退避等待下一次尝试
)

// jobTrack 是单条未终态事件的运行期追踪信息。
type jobTrack struct {
	offset    int64
	eventID   string
	key       string
	phase     jobPhase
	attempts  int
	waitUntil time.Time // phase==retrying 时下次尝试时刻
	lastError string
}

// jobTracker 由在线运行时实现，供 processEvent 汇报重试阶段变化；
// 离线重放传 nil。追踪只影响可观测性与完成判定，不改变任何处理语义。
type jobTracker interface {
	onRetry(attempt int, reason string, waitUntil time.Time)
	onProcess(nextAttempt int)
	onAbort(attempt int, reason string)
}

// GapInfo 描述水位之后一个尚未到达终态的位置（空洞）。
type GapInfo struct {
	Offset    int64     `json:"offset"`
	EventID   string    `json:"event_id"`
	Key       string    `json:"key"`
	Phase     string    `json:"phase"` // queued / processing / retrying / published / stopped
	Attempts  int       `json:"attempts,omitempty"`
	WaitUntil time.Time `json:"wait_until,omitempty"`
	Reason    string    `json:"reason,omitempty"`
}

// KeyProgress 是单个业务键的剩余进度。
type KeyProgress struct {
	Key           string `json:"key"`
	Total         int    `json:"total"`
	Processed     int    `json:"processed"`
	Dead          int    `json:"dead"`
	Remaining     int    `json:"remaining"` // 尚未终态（排队/处理中/重试/未派发）
	Processing    int    `json:"processing,omitempty"`
	WaitingRetry  int    `json:"waiting_retry,omitempty"`
	Queued        int    `json:"queued,omitempty"`
	NotDispatched int    `json:"not_dispatched,omitempty"`
}

// ProgressReport 是“处理到哪了、凭什么判定完成”的可解释进度快照。
type ProgressReport struct {
	LogEnd          int64         `json:"log_end"`          // 已持久化事件总数（下一条偏移）
	CommittedOffset int64         `json:"committed_offset"` // 已连续终态水位
	Processed       int           `json:"processed"`        // 终态=成功的偏移数（含重复投递）
	Dead            int           `json:"dead"`             // 终态=死信的偏移数
	Processing      int           `json:"processing"`       // 正在执行 Handler
	WaitingRetry    int           `json:"waiting_retry"`    // 退避中等待重试
	Queued          int           `json:"queued"`           // 已派发但尚未开始处理
	NotDispatched   int           `json:"not_dispatched"`   // 已发布但 dispatcher 尚未读到
	InFlight        int           `json:"in_flight"`        // processing+retrying+queued
	Complete        bool          `json:"complete"`         // 全部已发布事件均已终态
	IdleTarget      int64         `json:"idle_target"`      // 本次判定覆盖到的日志末尾偏移
	Gaps            []GapInfo     `json:"gaps"`             // 水位之后尚未终态的位置（抽样）
	Keys            []KeyProgress `json:"keys"`             // 各业务键剩余情况
	Basis           string        `json:"basis"`            // 判定依据（人读）
	GeneratedAt     time.Time     `json:"generated_at"`
}

// WaitUntilIdle 阻塞直到“调用时已发布（fsync 入日志）”的全部事件都到达
// 终态（成功或死信）并持久化：没有任何事件仍在处理、退避重试或排队，
// 水位连续覆盖到调用时刻的日志末尾。
//
// 运行中与停机后均可调用：
//   - 运行中：等待消费者实际处理完后返回，返回后立即查看状态即为完整结果；
//   - 停机后：以持久化日志/快照做一次性判定——已经全部终态则立即返回 nil；
//     尚有未终态事件时返回 ErrPipelineNotRunning（停机状态不会自行推进，
//     报告随附在错误的 Progress 中，可 Start 后再等待），报告可通过
//     Progress() 另行获取。
//
// 调用之后由其他 goroutine 新追加的事件不在本次等待范围内；需要把新追加
// 的批次也算进来，应在追加完成后再次调用 WaitUntilIdle。
//
// ctx 超时/取消时返回 ctx 的错误，此时返回不代表“已完成”，Progress 仍可
// 如实展示差距。
func (p *Pipeline) WaitUntilIdle(ctx context.Context) error {
	target := p.log.NextOffset()
	if p.rt == nil {
		// 停机后不会有任何作业推进；已全部终态则立即确认，否则明确告知
		// “停机状态无法自行完成”，而不是轮询等待一个永不到来的时刻。
		rep := p.progressAt(ctx, target)
		if rep.Complete {
			return nil
		}
		return fmt.Errorf("%w: %d/%d offsets terminal, %d unfinished (processing=%d retrying=%d queued=%d unpublished=%d)",
			ErrPipelineNotRunning, rep.CommittedOffset, rep.LogEnd,
			rep.Processing+rep.WaitingRetry+rep.Queued+rep.NotDispatched,
			rep.Processing, rep.WaitingRetry, rep.Queued, rep.NotDispatched)
	}
	return p.rt.waitIdleUntil(ctx, target)
}

// Progress 返回当前可解释的进度快照（运行中与停机后均可调用）。
func (p *Pipeline) Progress() ProgressReport {
	return p.progressAt(context.Background(), p.log.NextOffset())
}

// progressAt 汇总出截至 target（调用时刻的日志末尾）的进度。
//
// 判定事实来自两处，且互不猜测：
//   - StateStore：每个偏移是否已有终态（成功/死信）——这是“完成”的权威依据；
//   - 运行期 jobs / readPos：未终态偏移当前在哪个阶段（处理/重试/排队/未读）。
//
// “完成”当且仅当 committed==target（水位连续覆盖全部已发布偏移）。
func (p *Pipeline) progressAt(ctx context.Context, target int64) ProgressReport {
	terminal := p.store.TerminalOffsets()
	committed := p.store.Committed()

	rep := ProgressReport{
		LogEnd:          p.log.NextOffset(),
		CommittedOffset: committed,
		IdleTarget:      target,
		Gaps:            []GapInfo{},
		Keys:            []KeyProgress{},
		GeneratedAt:     time.Now(),
	}

	var live map[int64]*jobTrack
	var readPos int64
	running := p.rt != nil
	if running {
		live = p.rt.snapshotJobs()
		readPos = p.rt.dispatchedUpTo()
	}

	keyAgg := make(map[string]*KeyProgress)
	// 需要从日志补充事件 id/key（jobs 只覆盖已派发未终态的偏移）。
	ensureKey := func(k string) *KeyProgress {
		kp := keyAgg[k]
		if kp == nil {
			kp = &KeyProgress{Key: k}
			keyAgg[k] = kp
		}
		return kp
	}

	// 先汇总全部已发布事件与终态，逐偏移统计，保证每类计数相加==LogEnd。
	for off := int64(0); off < target; off++ {
		var evID, key string
		if se, err := p.log.ReadAt(off); err == nil {
			evID, key = se.Event.ID, se.Event.Key
		} else {
			key = "<unreadable>"
			evID = fmt.Sprintf("offset-%d", off)
		}
		kp := ensureKey(key)
		kp.Total++

		if rec, ok := terminal[off]; ok {
			if rec.Status == StatusDead {
				rep.Dead++
				kp.Dead++
			} else {
				rep.Processed++
				kp.Processed++
			}
			continue
		}

		// 未终态：确定阶段。
		kp.Remaining++
		gap := GapInfo{Offset: off, EventID: evID, Key: key}
		switch jt := live[off]; {
		case jt != nil:
			gap.Attempts = jt.attempts
			gap.Reason = jt.lastError
			switch jt.phase {
			case phaseProcessing:
				gap.Phase = "processing"
				rep.Processing++
				kp.Processing++
			case phaseRetrying:
				gap.Phase = "retrying"
				gap.WaitUntil = jt.waitUntil
				rep.WaitingRetry++
				kp.WaitingRetry++
			default:
				gap.Phase = "queued"
				rep.Queued++
				kp.Queued++
			}
		case running && off < readPos:
			// dispatcher 已读入但当前不在 jobs：通常是提交收尾的瞬时窗口；
			// 保守按排队对待，绝不在这种窗口误报完成。
			gap.Phase = "queued"
			rep.Queued++
			kp.Queued++
		case running:
			gap.Phase = "published"
			rep.NotDispatched++
			kp.NotDispatched++
		default:
			gap.Phase = "stopped"
			rep.NotDispatched++
			kp.NotDispatched++
		}
		if len(rep.Gaps) < gapSampleLimit {
			rep.Gaps = append(rep.Gaps, gap)
		}
	}

	rep.InFlight = rep.Processing + rep.WaitingRetry + rep.Queued
	unfinished := rep.InFlight + rep.NotDispatched
	rep.Complete = p.store.AllTerminalUntil(target) && unfinished == 0 && target >= 0

	keys := make([]KeyProgress, 0, len(keyAgg))
	for _, kp := range keyAgg {
		keys = append(keys, *kp)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].Key < keys[j].Key })
	rep.Keys = keys
	rep.Basis = describeBasis(rep, running, readPos)

	_ = ctx // ctx 预留给未来的有界扫描；当前快照为纯本地读。
	return rep
}

// describeBasis 生成“凭什么判定完成/没完成”的人读依据。
func describeBasis(rep ProgressReport, running bool, readPos int64) string {
	switch {
	case rep.Complete:
		return fmt.Sprintf(
			"committed watermark %d == log end %d: every published offset is terminal (processed/dead), "+
				"no queued/processing/retrying/unread events; last terminal offset=%d, no gaps",
			rep.CommittedOffset, rep.LogEnd, rep.CommittedOffset-1)
	case rep.CommittedOffset >= rep.LogEnd && !running:
		return fmt.Sprintf("committed=%d >= log end=%d but pipeline is stopped; state is final for published events",
			rep.CommittedOffset, rep.LogEnd)
	}
	state := "stopped"
	if running {
		state = "running"
	}
	return fmt.Sprintf(
		"%s: committed watermark=%d, log end=%d, first gap at offset=%d (event=%s key=%s phase=%s); "+
			"unfinished: processing=%d waiting_retry=%d queued=%d not_dispatched=%d; dispatched_upto=%d",
		state, rep.CommittedOffset, rep.LogEnd,
		rep.Gaps[0].Offset, rep.Gaps[0].EventID, rep.Gaps[0].Key, rep.Gaps[0].Phase,
		rep.Processing, rep.WaitingRetry, rep.Queued, rep.NotDispatched, readPos)
}
