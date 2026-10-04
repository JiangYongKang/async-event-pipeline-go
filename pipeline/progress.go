package pipeline

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"
)

// KeyProgress 描述单个业务键的剩余工作量。
type KeyProgress struct {
	Key       string `json:"key"`
	Published int64  `json:"published"` // 日志中该键已发布的事件条数（含重复投递副本）
	Terminal  int64  `json:"terminal"`  // 其中已到达终态（成功/死信/重复标记）的条数
	Remaining int64  `json:"remaining"` // 尚未到达终态的条数
}

// Progress 是一份可解释的管道进度说明（某一时刻的观测快照）。
//
// 计数恒等式（每个日志偏移恰好被归入一类）：
//
//	LogEndOffset = Succeeded + Dead + Handling + Retrying + Waiting + Unread
//
// 其中 Succeeded/Dead 按“已终态的投递偏移”计数（重复投递副本沿用其首次
// 终态的归类）；Handling/Retrying/Waiting 是运行中管道的在途细分，
// Unread 是已发布但 dispatcher 尚未读到的条数。停机时在途三项恒为 0，
// 未完成事件全部计入 Unread。
type Progress struct {
	ObservedAt time.Time `json:"observed_at"`
	Running    bool      `json:"running"`

	// LogEndOffset 观测时刻日志中已发布事件总数（= 下一偏移）。
	LogEndOffset int64 `json:"log_end_offset"`
	// DispatchedOffset dispatcher 已顺序读入到的位置（运行中才有意义）。
	DispatchedOffset int64 `json:"dispatched_offset"`
	// CommittedOffset 已连续提交的终态水位。
	CommittedOffset int64 `json:"committed_offset"`

	Succeeded int64 `json:"succeeded"` // 已成功处理的投递偏移数
	Dead      int64 `json:"dead"`      // 已进入死信的投递偏移数

	Handling int64 `json:"handling"` // 正在执行 Handler/提交
	Retrying int64 `json:"retrying"` // 终态前退避等待重试
	Waiting  int64 `json:"waiting"`  // 已入键队列、等待同键前序事件
	Unread   int64 `json:"unread"`   // 已发布但尚未开始处理（停机时=全部未完成）

	// GapOffsets 水位之前仍无终态记录的偏移（即“缺口”）；由于水位只能
	// 连续推进，它恰好等于所有未终态偏移，按升序排列。
	GapOffsets []int64 `json:"gap_offsets"`

	// Keys 每个业务键各自的已发布/已终态/剩余数量（按键名排序）。
	Keys []KeyProgress `json:"keys"`

	// Complete 为 true 当且仅当：已发布事件全部到达终态、无在途/重试/排队/
	// 未读事件，且水位已连续推进到日志末尾（无缺口）。
	Complete bool   `json:"complete"`
	Basis    string `json:"basis"` // 判定依据（人读）
}

// maxGapsInText 限制多行文本中直接展开的缺口偏移数量，其余以计数概括。
const maxGapsInText = 20

// String 返回多行、可直接打印的进度说明。
func (pr Progress) String() string {
	var b strings.Builder
	state := "stopped"
	if pr.Running {
		state = "running"
	}
	fmt.Fprintf(&b, "progress(%s) logEnd=%d dispatched=%d committed=%d complete=%t\n",
		state, pr.LogEndOffset, pr.DispatchedOffset, pr.CommittedOffset, pr.Complete)
	fmt.Fprintf(&b, "  terminal : succeeded=%d dead=%d\n", pr.Succeeded, pr.Dead)
	fmt.Fprintf(&b, "  in-flight: handling=%d retrying=%d waiting=%d unread=%d\n",
		pr.Handling, pr.Retrying, pr.Waiting, pr.Unread)
	switch {
	case len(pr.GapOffsets) == 0:
		fmt.Fprintf(&b, "  gaps    : none (committed watermark contiguous to log end)\n")
	case len(pr.GapOffsets) <= maxGapsInText:
		fmt.Fprintf(&b, "  gaps    : %v (%d offset(s) without terminal record)\n",
			pr.GapOffsets, len(pr.GapOffsets))
	default:
		fmt.Fprintf(&b, "  gaps    : %v ... (+%d more, %d total without terminal record)\n",
			pr.GapOffsets[:maxGapsInText], len(pr.GapOffsets)-maxGapsInText, len(pr.GapOffsets))
	}
	if len(pr.Keys) > 0 {
		b.WriteString("  per-key remaining:\n")
		for _, k := range pr.Keys {
			fmt.Fprintf(&b, "    %-20s published=%d terminal=%d remaining=%d\n",
				k.Key, k.Published, k.Terminal, k.Remaining)
		}
	}
	fmt.Fprintf(&b, "  verdict : %s\n", pr.Basis)
	return b.String()
}

// computeProgress 汇总日志、状态存储与运行时的瞬时观测，生成进度说明。
// 每个日志偏移恰好归入“成功/死信/处理中/重试中/排队/未读”之一。
func computeProgress(running bool, rt *runtime, el *EventLog, st *StateStore) Progress {
	now := time.Now()
	end := el.NextOffset()
	committed := st.Committed()

	pr := Progress{
		ObservedAt:      now,
		Running:         running,
		LogEndOffset:    end,
		CommittedOffset: committed,
	}

	var phases map[int64]string
	var queued map[int64]struct{}
	var dispatched int64
	if running && rt != nil {
		dispatched = rt.dispatchFrom + rt.ctr.dispatched.Load()
		pr.DispatchedOffset = dispatched
		phases = rt.phaseSnapshot()
		queued = rt.queuedOffsets()
	}

	// 逐偏移分类；同时收集每键已发布数（需要解析日志得到键）。
	keys, err := el.PublishedKeys(0)
	if err != nil {
		// 日志在打开时已完整校验，此处出错只可能是 I/O 异常：保守地不判完成。
		pr.Basis = fmt.Sprintf("progress unavailable: read event log: %v", err)
		return pr
	}
	published := make(map[string]int64)
	for off := int64(0); off < end; off++ {
		var key string
		if int(off) < len(keys) {
			key = keys[off]
			published[key]++
		}
		if status, ok := st.TerminalAt(off); ok {
			if status == StatusDead {
				pr.Dead++
			} else {
				pr.Succeeded++
			}
			continue
		}
		// 未终态偏移：它就是水位前方的缺口。
		pr.GapOffsets = append(pr.GapOffsets, off)
		switch {
		case !running:
			// 停机后不存在在途 worker：未完成的都要等下次 Start 后处理。
			pr.Unread++
		case off >= dispatched:
			pr.Unread++
		default:
			// 已被 dispatcher 读出但尚无终态：按在途子状态精确归类。
			switch phases[off] {
			case phaseRetrying:
				pr.Retrying++
			case phaseHandling:
				pr.Handling++
			default:
				if _, ok := queued[off]; ok {
					pr.Waiting++
				} else {
					// 已派发、已出队但状态记账尚未登记的极短窗口（或停机中断
					// 留下的未终态 worker）：它仍是未完成的在途工作。
					pr.Handling++
				}
			}
		}
	}

	// 每键终态数：逐偏移结合日志中的键统计（重复副本沿用首次终态归类）。
	terminal := make(map[string]int64)
	for off := int64(0); off < end; off++ {
		if _, ok := st.TerminalAt(off); !ok {
			continue
		}
		if int(off) < len(keys) {
			terminal[keys[off]]++
		}
	}
	keyNames := make([]string, 0, len(published))
	for k := range published {
		keyNames = append(keyNames, k)
	}
	sort.Strings(keyNames)
	pr.Keys = make([]KeyProgress, 0, len(keyNames))
	for _, k := range keyNames {
		pr.Keys = append(pr.Keys, KeyProgress{
			Key:       k,
			Published: published[k],
			Terminal:  terminal[k],
			Remaining: published[k] - terminal[k],
		})
	}

	pr.Complete = len(pr.GapOffsets) == 0 && committed == end &&
		pr.Handling+pr.Retrying+pr.Waiting+pr.Unread == 0
	switch {
	case pr.Complete:
		pr.Basis = fmt.Sprintf(
			"idle: committed=%d reached logEnd=%d with no gap; succeeded=%d dead=%d, no handling/retrying/waiting/unread event",
			committed, end, pr.Succeeded, pr.Dead)
	case !running:
		pr.Basis = fmt.Sprintf(
			"not idle while stopped: committed=%d logEnd=%d, %d offset(s) unprocessed; reopen with Start to recover",
			committed, end, len(pr.GapOffsets))
	default:
		firstGap := int64(-1)
		if len(pr.GapOffsets) > 0 {
			firstGap = pr.GapOffsets[0]
		}
		pr.Basis = fmt.Sprintf(
			"not idle: committed=%d logEnd=%d dispatched=%d, first gap=%d, in-flight handling=%d retrying=%d waiting=%d unread=%d",
			committed, end, dispatched, firstGap,
			pr.Handling, pr.Retrying, pr.Waiting, pr.Unread)
	}
	return pr
}

// Progress 返回当前进度说明（运行中与停机后均可调用）。
func (p *Pipeline) Progress() Progress {
	running := p.rt != nil
	return computeProgress(running, p.rt, p.log, p.store)
}

// WaitUntilIdle 阻塞直到“当前已发布的事件全部处理到位”：
// 每条已 fsync 入日志的事件都到达终态（成功或死信），没有事件仍在
// 处理、退避重试、键队列排队或尚未被 dispatcher 读到，水位连续推进到
// 日志末尾且无缺口。等待期间新追加（Publish）的事件同样纳入等待范围，
// 因此重启恢复、运行中追加新一批事件都适用。
//
// 管道未运行时：若所有已发布事件均已终态（例如 Drain 之后）立即返回 nil；
// 若尚有未处理事件，返回 ErrPipelineNotRunning——此时请 Start 恢复消费后
// 再等待（停机状态不可能自行产生新进展）。可通过 Progress() 查看差距。
func (p *Pipeline) WaitUntilIdle(ctx context.Context) error {
	ticker := time.NewTicker(p.cfg.PollInterval)
	defer ticker.Stop()
	for {
		rt := p.rt // 与 Drain 并发时只读一次：rt 置 nil 后按停机判定处理
		pr := computeProgress(rt != nil, rt, p.log, p.store)
		if pr.Complete {
			if rt != nil {
				rt.logger.Infof("wait-idle verdict=idle progress=%d basis=%s", pr.CommittedOffset, pr.Basis)
			}
			return nil
		}
		if rt == nil {
			return fmt.Errorf("%w: %s", ErrPipelineNotRunning, pr.Basis)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-rt.notify:
		case <-ticker.C:
		}
	}
}
