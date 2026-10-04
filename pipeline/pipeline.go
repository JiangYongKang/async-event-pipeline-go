package pipeline

import (
	"context"
	"path/filepath"
	"time"
)

const (
	defaultParallelism  = 4
	defaultMaxAttempts  = 5
	defaultRetryBackoff = 10 * time.Millisecond
	defaultMaxBackoff   = 500 * time.Millisecond
	defaultPollInterval = 20 * time.Millisecond
	logFileName         = "events.log"
	stateFileName       = "state.snapshot.json"
)

// Config 控制管道运行参数。
type Config struct {
	// DataDir 存放 events.log 与 state.snapshot.json 的本地目录。
	DataDir string
	// Parallelism 跨业务键并行度上限（同键仍严格按产生顺序串行处理）。
	Parallelism int
	// MaxAttempts 单事件瞬态错误的最大尝试次数（含首次）；超过即死信。
	MaxAttempts int
	// RetryBackoff 初次重试退避，之后指数递增并封顶 MaxRetryBackoff。
	RetryBackoff    time.Duration
	MaxRetryBackoff time.Duration
	// PollInterval 文件轮询间隔（作为新事件唤醒信号丢失时的兜底）。
	PollInterval time.Duration
	// NewBusiness 构造空业务状态；nil 时使用内置 *KVState。
	NewBusiness func() BusinessState
	// Logger 运行日志；nil 时输出到 stderr，DiscardLogger 可静默。
	Logger Logger
}

// DefaultConfig 返回以 dataDir 为数据目录的默认配置。
func DefaultConfig(dataDir string) Config {
	return Config{
		DataDir:         dataDir,
		Parallelism:     defaultParallelism,
		MaxAttempts:     defaultMaxAttempts,
		RetryBackoff:    defaultRetryBackoff,
		MaxRetryBackoff: defaultMaxBackoff,
		PollInterval:    defaultPollInterval,
	}
}

func (c *Config) withDefaults() Config {
	cfg := *c
	if cfg.Parallelism <= 0 {
		cfg.Parallelism = defaultParallelism
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = defaultMaxAttempts
	}
	if cfg.RetryBackoff <= 0 {
		cfg.RetryBackoff = defaultRetryBackoff
	}
	if cfg.MaxRetryBackoff <= 0 {
		cfg.MaxRetryBackoff = defaultMaxBackoff
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = defaultPollInterval
	}
	if cfg.Logger == nil {
		cfg.Logger = NewLogger(nil)
	}
	return cfg
}

// Stats 是管道的可观测计数（快照）。
type Stats struct {
	NextOffset      int64 `json:"next_offset"`
	CommittedOffset int64 `json:"committed_offset"`
	InFlight        int   `json:"in_flight"`
	ProcessedTotal  int64 `json:"processed_total"`
	DeadTotal       int64 `json:"dead_total"`
	DuplicateTotal  int64 `json:"duplicate_total"`
	RetryTotal      int64 `json:"retry_total"`
}

// Pipeline 组合事件日志、状态存储与并发消费运行时。
type Pipeline struct {
	cfg     Config
	log     *EventLog
	store   *StateStore
	handler Handler
	rt      *runtime
}

// Open 打开/创建本地管道（日志与状态文件均位于 cfg.DataDir）。
func Open(cfg Config, handler Handler) (*Pipeline, error) {
	if handler == nil {
		handler = NewCountHandler()
	}
	cfg = cfg.withDefaults()
	el, err := OpenEventLog(filepath.Join(cfg.DataDir, logFileName))
	if err != nil {
		return nil, err
	}
	st, err := OpenStateStore(filepath.Join(cfg.DataDir, stateFileName), cfg.NewBusiness)
	if err != nil {
		_ = el.Close()
		return nil, err
	}
	// 用日志对持久化进度对账自愈：补回重启丢失的“重复投递偏移终态登记”，
	// 让水位与按偏移的终态索引重新自洽（必须在消费者启动前完成）。
	if fixed, err := st.ReconcileWithLog(el); err != nil {
		_ = el.Close()
		_ = st.Close()
		return nil, err
	} else if fixed {
		cfg.Logger.Infof("reconcile complete progress=%d logEnd=%d basis=rebuilt duplicate-offset terminal index from log+idempotency-table",
			st.Committed(), el.NextOffset())
	}
	return &Pipeline{
		cfg:     cfg,
		log:     el,
		store:   st,
		handler: handler,
	}, nil
}

// Publish 持久化一条事件（可靠投递入口），返回其偏移。
// 事件在 Append 返回时已 fsync，崩溃不丢；同 ID 重复投递在消费端幂等去重。
func (p *Pipeline) Publish(ev Event) (int64, error) {
	return p.log.Append(ev)
}

// Start 启动消费运行时：从已提交水位恢复，先补齐回放缺口再实时消费。
func (p *Pipeline) Start(ctx context.Context) error {
	rt, err := startRuntime(ctx, p.cfg, p.log, p.store, p.handler)
	if err != nil {
		return err
	}
	p.rt = rt
	return nil
}

// Drain 停止派发新事件，等待在途事件到达终态并落盘后返回（优雅停机）。
// 重试退避会被中断后立即再试一次；ctx 超时则返回其错误，状态仍保持
// “已完成即已持久化”，下次 Start 从水位继续，不会丢事件或重复生效。
func (p *Pipeline) Drain(ctx context.Context) error {
	if p.rt == nil {
		return ErrPipelineNotRunning
	}
	err := p.rt.drain(ctx)
	p.rt = nil
	return err
}

// WaitForOffset 阻塞直到提交水位推进到 offset（即 offset 之前的事件都已
// 到达终态并持久化）。offset 超出日志当前末尾时返回 ErrOffsetBeyondEnd。
func (p *Pipeline) WaitForOffset(ctx context.Context, offset int64) error {
	if p.rt == nil {
		// 未运行时直接对照状态文件判定。
		if offset > p.log.NextOffset() {
			return ErrOffsetBeyondEnd
		}
		return waitStoreOffset(ctx, p.store, offset, p.cfg.PollInterval)
	}
	return p.rt.waitOffset(ctx, offset)
}

// Seek 在运行中等待水位推进到 offset。这是“向前”的同步屏障：
//   - offset 早于已提交水位：明确拒绝（ErrRollbackRejected），回退请用
//     Replay 显式表达“重新处理”的意图；
//   - offset 超出日志末尾：拒绝（ErrOffsetBeyondEnd），不得静默跳过；
//   - 其余情况阻塞等待对应区间全部处理完，绝不跳过任何事件。
func (p *Pipeline) Seek(ctx context.Context, offset int64) error {
	if p.rt == nil {
		return ErrPipelineNotRunning
	}
	return p.rt.seek(ctx, offset)
}

// Replay 在停机状态下显式回到日志最早位置重放整条日志。
//
// 重放会清空消费进度、幂等/死信记录与业务效果快照，然后重新顺序执行
// 全部历史事件；因为处理是幂等的（重复事件 ID 不再生效），最终业务
// 状态与首次顺序执行一致，死信会按当前 Handler 语义重新判定。
// 回退到不可恢复位置（日志最早位置之前）的请求会被明确拒绝。
func (p *Pipeline) Replay(ctx context.Context, offset int64) error {
	if p.rt != nil {
		return ErrPipelineRunning
	}
	return replayOffline(ctx, p.cfg, p.log, p.store, p.handler, offset)
}

// Stats 返回当前计数快照。
func (p *Pipeline) Stats() Stats {
	snap := p.store.Snapshot()
	st := Stats{
		NextOffset:      p.log.NextOffset(),
		CommittedOffset: snap.CommittedOffset,
		DeadTotal:       int64(len(snap.Dead)),
	}
	if p.rt != nil {
		st.InFlight = p.rt.inFlight()
		st.ProcessedTotal = p.rt.processedTotal()
		st.DuplicateTotal = p.rt.duplicateTotal()
		st.RetryTotal = p.rt.retryTotal()
	}
	return st
}

// Snapshot 返回状态快照（含业务效果与死信列表）。
func (p *Pipeline) Snapshot() StateSnapshot { return p.store.Snapshot() }

// StateStore / EventLog 暴露给需要精细操作的调用方（主要用于测试）。
func (p *Pipeline) StateStore() *StateStore { return p.store }
func (p *Pipeline) EventLog() *EventLog     { return p.log }

// Close 关闭管道（若在运行需先 Drain）。
func (p *Pipeline) Close() error {
	if p.rt != nil {
		return ErrPipelineRunning
	}
	err1 := p.log.Close()
	err2 := p.store.Close()
	if err1 != nil {
		return err1
	}
	return err2
}
