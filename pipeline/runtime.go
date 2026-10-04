package pipeline

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// dispatchBuffer 是 dispatcher 从日志读事件的缓冲大小；仅用于平滑突发，
// 事件真相始终在日志文件中，不依赖内存通道传递。
const dispatchBuffer = 64

// keyQueue 是同一业务键的 FIFO 待处理队列；busy 表示队首事件正在处理，
// 只有它到达终态（成功或死信）后才会调度同键下一条，从而保证同键严格
// 按日志中的产生顺序生效。
type keyQueue struct {
	events []StoredEvent
	busy   bool
}

type counters struct {
	processed atomic.Int64
	dead      atomic.Int64
	duplicate atomic.Int64
	retry     atomic.Int64
	inflight  atomic.Int64
}

// runtime 是并发消费运行时。
//
// 并发模型：
//   - 单个 dispatcher goroutine 顺序读取日志（先回放后实时），因此事件
//     进入调度器的顺序就是产生顺序；投递、消费与重试并发时也不会乱序；
//   - dispatcher 为每个业务键维护 FIFO 队列，同键事件一次只处理一条；
//   - Parallelism 个许可控制跨键并行度，不同业务键的事件并行推进；
//   - 每个事件到终态后通过 StateStore 的乐观版本控制提交业务效果，
//     基线过期则基于最新状态重跑 Handle，绝不覆盖其他键刚提交的效果。
type runtime struct {
	cfg     Config
	log     *EventLog
	store   *StateStore
	handler Handler
	logger  Logger

	runCtx    context.Context
	runCancel context.CancelFunc

	mu        sync.Mutex
	queues    map[string]*keyQueue
	keyOrder  []string
	permits   chan struct{}
	wakeSched chan struct{}

	streamCancel context.CancelFunc
	dispDone     chan struct{}
	schedDone    chan struct{}
	jobsWG       sync.WaitGroup
	draining     bool
	drainWake    chan struct{}

	notify chan struct{} // 水位推进信号（cap=1，丢失由调用方轮询兜底）
	ctr    counters

	// readPos 是 dispatcher 已经读入管道（dispatchOne）的连续位置：
	// [readPos, logEnd) 即“已发布但还没开始处理”的事件。dispatcher 单
	// goroutine 顺序写，进度查询并发读，故用原子量。
	readPos atomic.Int64
	// jobs 追踪所有已派发但尚未终态的偏移所处阶段（queued/processing/retrying）。
	jobsMu sync.Mutex
	jobs   map[int64]*jobTrack
}

func startRuntime(ctx context.Context, cfg Config, el *EventLog, st *StateStore, h Handler) (*runtime, error) {
	// ctx 仅表示“启动动作”的生命周期；运行生命周期由 Drain/Close 控制，
	// 避免调用方的请求 context 取消后消费进程意外停止。
	runCtx, runCancel := context.WithCancel(context.Background())
	rt := &runtime{
		cfg:       cfg,
		log:       el,
		store:     st,
		handler:   h,
		logger:    cfg.Logger,
		runCtx:    runCtx,
		runCancel: runCancel,
		queues:    make(map[string]*keyQueue),
		permits:   make(chan struct{}, cfg.Parallelism),
		wakeSched: make(chan struct{}, 1),
		dispDone:  make(chan struct{}),
		schedDone: make(chan struct{}),
		drainWake: make(chan struct{}),
		notify:    make(chan struct{}, 1),
		jobs:      make(map[int64]*jobTrack),
	}
	rt.readPos.Store(st.Committed())
	streamCtx, streamCancel := context.WithCancel(runCtx)
	rt.streamCancel = streamCancel
	go rt.dispatch(streamCtx, st.Committed())
	go rt.schedulerLoop(runCtx)
	return rt, nil
}

// dispatch 是唯一的事件读取者：顺序读日志（Stream 退出时关闭 ch），
// 并按业务键入队。
func (rt *runtime) dispatch(ctx context.Context, from int64) {
	defer close(rt.dispDone)
	ch := make(chan StoredEvent, dispatchBuffer)
	go func() {
		_ = rt.log.Stream(ctx, from, ch, rt.cfg.PollInterval)
	}()
	for se := range ch {
		rt.dispatchOne(se)
	}
}

// dispatchOne 处理 dispatcher 读到的一条事件：先做幂等快判，再按键入队。
func (rt *runtime) dispatchOne(se StoredEvent) {
	// dispatcher 是唯一顺序读取者：连续推进已读位置（重复事件也已读入，
	// 且在下面的同步提交后立即终态，不进入 jobs 追踪）。
	rt.readPos.Store(se.Offset + 1)
	if rec, ok := rt.store.Lookup(se.Event.ID); ok {
		// 事件 ID 已有终态（成功或死信）：业务效果绝不第二次生效。
		rt.ctr.duplicate.Add(1)
		if err := rt.store.MarkDuplicate(se.Offset, rec); err != nil {
			rt.logger.Errorf("event=%s offset=%d key=%s duplicate mark failed: %v",
				se.Event.ID, se.Offset, se.Event.Key, err)
			return
		}
		rt.logger.Infof("event=%s offset=%d key=%s verdict=duplicate progress=%d basis=idempotent-record(status=%s)",
			se.Event.ID, se.Offset, se.Event.Key, rt.store.Committed(), rec.Status)
		rt.signalWatermark()
		return
	}

	rt.mu.Lock()
	q := rt.queues[se.Event.Key]
	if q == nil {
		q = &keyQueue{}
		rt.queues[se.Event.Key] = q
		rt.keyOrder = append(rt.keyOrder, se.Event.Key)
	}
	q.events = append(q.events, se)
	rt.jobsWG.Add(1)
	rt.ctr.inflight.Add(1)
	rt.mu.Unlock()
	rt.trackAdd(se.Offset, se.Event.ID, se.Event.Key)
	rt.pokeScheduler()
}

// pokeScheduler 非阻塞唤醒调度器。
func (rt *runtime) pokeScheduler() {
	select {
	case rt.wakeSched <- struct{}{}:
	default:
	}
}

// schedulerLoop 反复尝试派发就绪的队首事件，直到运行上下文结束。
func (rt *runtime) schedulerLoop(ctx context.Context) {
	defer close(rt.schedDone)
	for {
		rt.schedule()
		select {
		case <-ctx.Done():
			return
		case <-rt.wakeSched:
		}
	}
}

// schedule 按派发顺序挑选“队首未忙”的键，占用一个并行许可后启动 worker。
// 许可不足立即停止：更早入队的键优先获得执行机会。
func (rt *runtime) schedule() {
	rt.mu.Lock()
	for _, key := range rt.keyOrder {
		q := rt.queues[key]
		if q == nil || q.busy || len(q.events) == 0 {
			continue
		}
		select {
		case rt.permits <- struct{}{}:
		default:
			rt.mu.Unlock()
			return
		}
		ev := q.events[0]
		q.busy = true
		rt.trackPhase(ev.Offset, phaseProcessing, 0, "", time.Time{})
		go rt.runJob(key, ev)
	}
	rt.mu.Unlock()
}

// runJob 在独立 goroutine 中把一个事件处理到终态（成功 / 死信）。
func (rt *runtime) runJob(key string, ev StoredEvent) {
	defer rt.jobsWG.Done()
	defer rt.ctr.inflight.Add(-1)
	defer func() { <-rt.permits }()
	defer rt.trackRemove(ev.Offset)

	tracker := &jobHook{rt: rt, offset: ev.Offset}
	res := processEvent(rt.runCtx, rt.cfg, rt.store, rt.handler, rt.logger, ev, &rt.ctr, rt.drainWake, tracker)

	rt.mu.Lock()
	q := rt.queues[key]
	if q != nil && len(q.events) > 0 && q.events[0].Offset == ev.Offset {
		q.events = q.events[1:]
	}
	if q != nil {
		q.busy = false
	}
	rt.mu.Unlock()

	if res.advanced {
		rt.signalWatermark()
	}
	rt.pokeScheduler()
}

func (rt *runtime) signalWatermark() {
	select {
	case rt.notify <- struct{}{}:
	default:
	}
}

// processResult 汇总单事件处理结果。
type processResult struct {
	duplicate bool
	dead      bool
	advanced  bool
	attempts  int
}

// processEvent 按“重试 + 乐观版本提交”把单个事件推进到终态。
// 在线 worker 与离线 Replay 顺序重放共用本函数，保证两条路径语义一致。
//
// 关键不变量：Handle 始终在最新业务状态的克隆上运行；任何失败（业务
// 失败、提交失败、上下文取消于终态之前）都直接丢弃克隆，事件保持未终态，
// 因而不会留下半完成状态。
func processEvent(ctx context.Context, cfg Config, st *StateStore, h Handler, lg Logger,
	ev StoredEvent, ctr *counters, drainWake <-chan struct{}, tracker jobTracker) processResult {

	var firstErrAt time.Time
	attempts := 0

	for {
		attempts++
		clone, baseVersion := st.BusinessAndVersion()
		err := safeHandle(ctx, h, ev, clone)
		if err == nil {
			rec := ProcessRecord{Offset: ev.Offset, EventID: ev.Event.ID, Key: ev.Event.Key}
			res, committed, perr := commitProcessedWithRetry(ctx, cfg, st, h, lg, ev, rec, clone, baseVersion)
			if perr == nil {
				if res.Duplicate {
					if ctr != nil {
						ctr.duplicate.Add(1)
					}
					lg.Infof("event=%s offset=%d key=%s verdict=duplicate progress=%d basis=concurrent-idempotent-record",
						ev.Event.ID, ev.Offset, ev.Event.Key, committed)
					return processResult{duplicate: true, advanced: res.Advanced, attempts: attempts}
				}
				if ctr != nil {
					ctr.processed.Add(1)
				}
				lg.Infof("event=%s offset=%d key=%s verdict=processed attempts=%d progress=%d basis=handler-success+atomic-snapshot(v=%d)",
					ev.Event.ID, ev.Offset, ev.Event.Key, attempts, committed, res.StateVersion)
				return processResult{advanced: res.Advanced, attempts: attempts}
			}
			// 提交失败：快照原子性保证旧快照完好，按瞬态错误重试。
			err = perr
			lg.Errorf("event=%s offset=%d key=%s commit failed, treat as transient: %v",
				ev.Event.ID, ev.Offset, ev.Event.Key, err)
		}

		ee := AsEventError(err)
		if firstErrAt.IsZero() {
			firstErrAt = time.Now()
		}
		if !ee.Retryable() || attempts >= cfg.MaxAttempts {
			kind := ee.Kind
			dead := DeadRecord{
				Offset:     ev.Offset,
				EventID:    ev.Event.ID,
				Key:        ev.Event.Key,
				Kind:       kind,
				Reason:     ee.Reason,
				Attempts:   attempts,
				FirstError: firstErrAt,
			}
			committed, advanced := commitDeadWithCAS(ctx, st, ev, dead, lg)
			if ctr != nil {
				ctr.dead.Add(1)
			}
			why := "max-attempts-exhausted"
			switch kind {
			case FailurePermanent:
				why = "permanent-error"
			case FailureInvalid:
				why = "invalid-payload"
			}
			lg.Errorf("event=%s offset=%d key=%s verdict=dead kind=%s reason=%q attempts=%d progress=%d basis=%s",
				ev.Event.ID, ev.Offset, ev.Event.Key, kind, ee.Reason, attempts, committed, why)
			return processResult{dead: true, advanced: advanced, attempts: attempts}
		}

		if ctr != nil {
			ctr.retry.Add(1)
		}
		backoff := backoffDelay(cfg, attempts)
		if tracker != nil {
			tracker.onRetry(attempts, ee.Reason, time.Now().Add(backoff))
		}
		lg.Warnf("event=%s offset=%d key=%s verdict=retry attempt=%d/%d progress=%d basis=transient-error(%q); backoff then reprocess on fresh clone",
			ev.Event.ID, ev.Offset, ev.Event.Key, attempts, cfg.MaxAttempts, st.Committed(), ee.Reason)
		if !sleepBackoff(ctx, backoff, drainWake) {
			if tracker != nil {
				tracker.onAbort(attempts, "context-canceled-before-terminal")
			}
			// 运行上下文在终态之前被取消：不判死信、不提交，重启后重新处理。
			lg.Warnf("event=%s offset=%d key=%s verdict=deferred progress=%d basis=context-canceled-before-terminal",
				ev.Event.ID, ev.Offset, ev.Event.Key, st.Committed())
			return processResult{attempts: attempts}
		}
		if tracker != nil {
			tracker.onProcess(attempts + 1)
		}
	}
}

// safeHandle 执行业务 Handler 并把 panic 转换为瞬态错误：panic 只影响
// 本次尝试（且发生在可丢弃克隆上），事件进入正常重试链路，不会让该键
// 队列永久卡死，也不会留下半完成效果。
func safeHandle(ctx context.Context, h Handler, ev StoredEvent, clone BusinessState) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = TransientError(fmt.Sprintf("handler panic: %v", r))
		}
	}()
	return h.Handle(ctx, ev, clone)
}

// commitProcessedWithRetry 执行成功提交；遇到 ErrStaleState 时在最新克隆上
// 重新施加事件效果并重试（乐观并发控制，不算业务失败、不消耗重试预算）。
func commitProcessedWithRetry(ctx context.Context, cfg Config, st *StateStore, h Handler, lg Logger,
	ev StoredEvent, rec ProcessRecord, clone BusinessState, baseVersion uint64) (CommitResult, int64, error) {
	for {
		res, err := st.CommitProcessed(rec, clone, baseVersion)
		if err == nil {
			return res, st.Committed(), nil
		}
		if !errors.Is(err, ErrStaleState) {
			return CommitResult{}, st.Committed(), err
		}
		lg.Infof("event=%s offset=%d key=%s verdict=reapply progress=%d basis=stale-state(v=%d), reapply on latest snapshot",
			ev.Event.ID, ev.Offset, ev.Event.Key, st.Committed(), res.StateVersion)
		clone, baseVersion = st.BusinessAndVersion()
		if rerr := safeHandle(ctx, h, ev, clone); rerr != nil {
			return CommitResult{}, st.Committed(), rerr
		}
	}
}

// commitDeadWithCAS 以最新业务状态（不含失败事件的效果）提交死信终态；
// 基线过期只需刷新克隆重试，因为死信不施加业务效果。
func commitDeadWithCAS(ctx context.Context, st *StateStore, ev StoredEvent,
	dead DeadRecord, lg Logger) (int64, bool) {
	for {
		clone, baseVersion := st.BusinessAndVersion()
		res, err := st.CommitDead(dead, clone, baseVersion)
		if err == nil {
			return st.Committed(), res.Advanced
		}
		if !errors.Is(err, ErrStaleState) {
			lg.Errorf("event=%s offset=%d key=%s dead-commit failed: %v",
				ev.Event.ID, ev.Offset, ev.Event.Key, err)
			if !sleepBackoff(ctx, 5*time.Millisecond, nil) {
				return st.Committed(), res.Advanced
			}
			continue
		}
	}
}

func backoffDelay(cfg Config, attempt int) time.Duration {
	d := cfg.RetryBackoff
	for i := 1; i < attempt; i++ {
		d *= 2
		if d >= cfg.MaxRetryBackoff {
			return cfg.MaxRetryBackoff
		}
	}
	if d > cfg.MaxRetryBackoff {
		return cfg.MaxRetryBackoff
	}
	return d
}

// sleepBackoff 在退避期间同时响应优雅停机（立即放行最后一轮处理）与取消。
// drainWake 为 nil 表示离线路径，只响应 ctx。返回 false 表示 ctx 已取消。
func sleepBackoff(ctx context.Context, d time.Duration, drainWake <-chan struct{}) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	if drainWake == nil {
		select {
		case <-ctx.Done():
			return false
		case <-t.C:
			return true
		}
	}
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	case <-drainWake:
		return true
	}
}

func (rt *runtime) inFlight() int         { return int(rt.ctr.inflight.Load()) }
func (rt *runtime) processedTotal() int64 { return rt.ctr.processed.Load() }
func (rt *runtime) duplicateTotal() int64 { return rt.ctr.duplicate.Load() }
func (rt *runtime) retryTotal() int64     { return rt.ctr.retry.Load() }

// ---- 未终态事件的运行期追踪（仅用于进度与完成判定）----

// jobHook 把 processEvent 的阶段变化写入 runtime.jobs。
type jobHook struct {
	rt     *runtime
	offset int64
}

func (h *jobHook) onRetry(attempt int, reason string, waitUntil time.Time) {
	h.rt.trackPhase(h.offset, phaseRetrying, attempt, reason, waitUntil)
}

func (h *jobHook) onProcess(nextAttempt int) {
	h.rt.trackPhase(h.offset, phaseProcessing, nextAttempt, "", time.Time{})
}

func (h *jobHook) onAbort(attempt int, reason string) {
	h.rt.trackPhase(h.offset, phaseQueued, attempt, reason, time.Time{})
}

func (rt *runtime) trackAdd(offset int64, eventID, key string) {
	rt.jobsMu.Lock()
	rt.jobs[offset] = &jobTrack{offset: offset, eventID: eventID, key: key, phase: phaseQueued}
	rt.jobsMu.Unlock()
}

func (rt *runtime) trackPhase(offset int64, phase jobPhase, attempt int, reason string, waitUntil time.Time) {
	rt.jobsMu.Lock()
	if jt := rt.jobs[offset]; jt != nil {
		jt.phase = phase
		if attempt > 0 {
			jt.attempts = attempt
		}
		if reason != "" {
			jt.lastError = reason
		}
		if !waitUntil.IsZero() {
			jt.waitUntil = waitUntil
		} else if phase != phaseRetrying {
			jt.waitUntil = time.Time{}
		}
	}
	rt.jobsMu.Unlock()
}

func (rt *runtime) trackRemove(offset int64) {
	rt.jobsMu.Lock()
	delete(rt.jobs, offset)
	rt.jobsMu.Unlock()
	// 终态提交发生在 remove 之前，这里再发一次信号，保证等待者能观察到
	// “水位到达且追踪表清空”的最终静止状态（信号丢失有轮询兜底）。
	rt.signalWatermark()
}

// snapshotJobs 返回当前未终态追踪记录的拷贝。
func (rt *runtime) snapshotJobs() map[int64]*jobTrack {
	rt.jobsMu.Lock()
	defer rt.jobsMu.Unlock()
	out := make(map[int64]*jobTrack, len(rt.jobs))
	for off, jt := range rt.jobs {
		cp := *jt
		out[off] = &cp
	}
	return out
}

func (rt *runtime) dispatchedUpTo() int64 { return rt.readPos.Load() }

// drain 停止派发并等待所有“调用时已发布”的事件到达终态并落盘。
//
// 先捕获日志末尾 target：target 之前的事件即使此刻还没被 dispatcher 读到，
// 也必须等到终态，杜绝“已 fsync 但尚未读入的事件在停机时被漏掉”。
// 随后中断重试退避、停止读取新事件，并等待在途作业清空；水位连续推进到
// target 后才返回。Drain 之后追加的事件不在本次停机保证范围内（流已停止）。
func (rt *runtime) drain(ctx context.Context) error {
	rt.mu.Lock()
	if rt.draining {
		rt.mu.Unlock()
		return nil
	}
	rt.draining = true
	target := rt.log.NextOffset()
	rt.mu.Unlock()

	close(rt.drainWake) // 中断所有重试退避，让在途事件立即做最后一轮尝试
	// 先等在途与未读事件全部到终态（此期间仍允许 dispatcher 把已发布事件读入），
	// 再停止读流；避免“先取消流、事件已在日志但未读入”造成的静默遗漏。
	if err := rt.waitIdleUntil(ctx, target); err != nil {
		rt.streamCancel()
		return err
	}
	rt.streamCancel()
	<-rt.dispDone
	rt.runCancel()
	rt.logger.Infof("drain complete progress=%d basis=all-published-through-offset-%d-reached-terminal-state",
		rt.store.Committed(), target)
	return nil
}

// waitIdleUntil 等待水位连续覆盖 target，并且追踪表中没有任何未终态事件。
// 水位条件（由 store 的提交逻辑保证）本身已是充分条件；追踪表为空是额外
// 校验，确保进度判定与等待判定用的是同一套事实，任何记账缺口都会暴露为
// 永不满足而非假阳性完成。
func (rt *runtime) waitIdleUntil(ctx context.Context, target int64) error {
	if target < 0 {
		return ErrOffsetBeforeEarliest
	}
	ticker := time.NewTicker(rt.cfg.PollInterval)
	defer ticker.Stop()
	for {
		if rt.store.AllTerminalUntil(target) && len(rt.snapshotJobs()) == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-rt.notify:
		case <-ticker.C:
		}
	}
}

// waitIdle 以“调用时刻的日志末尾”为目标等待空闲。目标在进入循环前捕获，
// 之后并发追加的事件不纳入本次等待——这正是“当前已经发布的事件全部处理完”
// 的语义边界；需要包含后续追加，可在追加后再次调用。
func (rt *runtime) waitIdle(ctx context.Context) error {
	target := rt.log.NextOffset()
	return rt.waitIdleUntil(ctx, target)
}

// waitOffset 阻塞直到提交水位 >= offset；结合信号与轮询兜底。
func (rt *runtime) waitOffset(ctx context.Context, offset int64) error {
	if offset < 0 {
		return ErrOffsetBeforeEarliest
	}
	if offset > rt.log.NextOffset() {
		return ErrOffsetBeyondEnd
	}
	ticker := time.NewTicker(rt.cfg.PollInterval)
	defer ticker.Stop()
	for {
		if rt.store.Committed() >= offset {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-rt.notify:
		case <-ticker.C:
		}
	}
}

// seek 是只允许向前的同步屏障；回退到已确认位置之前必须被明确拒绝。
func (rt *runtime) seek(ctx context.Context, offset int64) error {
	if offset < rt.store.Committed() {
		return ErrRollbackRejected
	}
	return rt.waitOffset(ctx, offset)
}

// waitStoreOffset 供未运行时使用：轮询等待水位。越界由调用方先行检查。
func waitStoreOffset(ctx context.Context, st *StateStore, offset int64, poll time.Duration) error {
	if offset < 0 {
		return ErrOffsetBeforeEarliest
	}
	ticker := time.NewTicker(poll)
	defer ticker.Stop()
	for {
		if st.Committed() >= offset {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// replayOffline 在停机状态下执行显式重放：
//  1. 校验目标位置必须是可恢复位置（当前实现：日志最早位置 0）；
//     更早 → ErrOffsetBeforeEarliest；超出末尾 → ErrOffsetBeyondEnd；
//  2. 清空进度/幂等/死信/业务效果快照；
//  3. 从日志头按顺序重新处理全部事件（含重试与死信判定），
//     幂等机制保证重复事件不二次生效，最终结果与首次顺序执行一致。
func replayOffline(ctx context.Context, cfg Config, el *EventLog, st *StateStore, h Handler, offset int64) error {
	if offset < el.Earliest() {
		return ErrOffsetBeforeEarliest
	}
	end := el.NextOffset()
	if offset > end {
		return ErrOffsetBeyondEnd
	}
	if offset != el.Earliest() {
		return ErrOffsetBeforeEarliest // 当前没有中段快照边界，拒绝从中间静默重放
	}
	if err := st.ResetForReplay(offset); err != nil {
		return err
	}
	cfg.Logger.Infof("replay start offset=%d end=%d basis=explicit-replay, state rebuilt from empty", offset, end)
	var ctr counters
	for pos := offset; pos < end; pos++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		se, err := el.ReadAt(pos)
		if err != nil {
			return err
		}
		// 与在线 dispatcher 相同的幂等快判：同一事件 ID 在日志中重复出现时，
		// 只在第一次出现处生效，后续出现仅补齐水位标记。
		if rec, ok := st.Lookup(se.Event.ID); ok {
			ctr.duplicate.Add(1)
			if err := st.MarkDuplicate(se.Offset, rec); err != nil {
				return err
			}
			cfg.Logger.Infof("event=%s offset=%d key=%s verdict=duplicate progress=%d basis=replay-idempotent-record",
				se.Event.ID, se.Offset, se.Event.Key, st.Committed())
			continue
		}
		processEvent(ctx, cfg, st, h, cfg.Logger, se, &ctr, nil, nil)
	}
	cfg.Logger.Infof("replay complete progress=%d processed=%d dead=%d duplicates=%d basis=sequential-reprocessing",
		st.Committed(), ctr.processed.Load(), ctr.dead.Load(), ctr.duplicate.Load())
	return nil
}
