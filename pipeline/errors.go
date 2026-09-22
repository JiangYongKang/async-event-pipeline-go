package pipeline

import "errors"

// 运行期哨兵错误。
var (
	// ErrEventIDEmpty / ErrEventKeyEmpty 拒绝缺少幂等标识或顺序键的事件。
	ErrEventIDEmpty  = errors.New("pipeline: event id is empty")
	ErrEventKeyEmpty = errors.New("pipeline: event key is empty")
	// ErrLogCorrupt 表示事件日志中间帧损坏（非末尾残缺），必须人工介入。
	ErrLogCorrupt = errors.New("pipeline: event log is corrupt")
	// ErrOffsetBeforeEarliest 拒绝回退到日志最早可恢复位置之前。
	ErrOffsetBeforeEarliest = errors.New("pipeline: offset is before earliest recoverable offset")
	// ErrOffsetBeyondEnd 拒绝定位到超出日志末尾的位置。
	ErrOffsetBeyondEnd = errors.New("pipeline: offset is beyond end of log")
	// ErrRollbackRejected 拒绝把已提交水位静默回退；需要重放请显式调用 Replay。
	ErrRollbackRejected   = errors.New("pipeline: rollback before committed offset is rejected; use Replay to reprocess explicitly")
	ErrPipelineRunning    = errors.New("pipeline: pipeline is already running")
	ErrPipelineClosed     = errors.New("pipeline: pipeline is closed")
	ErrPipelineNotRunning = errors.New("pipeline: pipeline is not running")
	// ErrStaleState 表示乐观并发提交时基线版本已过期，调用方应基于最新状态重试。
	ErrStaleState = errors.New("pipeline: stale business state, retry with latest snapshot")
)

// FailureKind 区分事件进入失败终态的原因类别，便于识别与归类。
type FailureKind string

const (
	// FailureTransient 可重试错误耗尽重试次数后进入死信。
	FailureTransient FailureKind = "transient"
	// FailurePermanent 业务明确判定不可重试，直接进入死信。
	FailurePermanent FailureKind = "permanent"
	// FailureInvalid 事件无法被 Handler 解析/接受（毒丸消息），不重试。
	FailureInvalid FailureKind = "invalid"
)

// EventError 由业务 Handler 返回；Kind 决定是否重试。
type EventError struct {
	Kind    FailureKind
	Reason  string
	Wrapped error
}

func (e *EventError) Error() string {
	if e == nil {
		return ""
	}
	s := "pipeline event error: " + string(e.Kind)
	if e.Reason != "" {
		s += ": " + e.Reason
	}
	if e.Wrapped != nil {
		s += ": " + e.Wrapped.Error()
	}
	return s
}

func (e *EventError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Wrapped
}

// Retryable 报告该错误是否允许重试。
func (e *EventError) Retryable() bool {
	return e != nil && e.Kind == FailureTransient
}

// AsEventError 从错误链中提取 *EventError；普通 error 包装为瞬态错误。
func AsEventError(err error) *EventError {
	if err == nil {
		return nil
	}
	var ee *EventError
	if errors.As(err, &ee) {
		return ee
	}
	return &EventError{Kind: FailureTransient, Reason: err.Error(), Wrapped: err}
}

// PermanentError 构造不可重试的业务错误。
func PermanentError(reason string, wrapped ...error) *EventError {
	return classified(FailurePermanent, reason, wrapped...)
}

// InvalidError 构造毒丸事件错误（不重试，直接死信）。
func InvalidError(reason string, wrapped ...error) *EventError {
	return classified(FailureInvalid, reason, wrapped...)
}

// TransientError 构造可重试错误。
func TransientError(reason string, wrapped ...error) *EventError {
	return classified(FailureTransient, reason, wrapped...)
}

func classified(kind FailureKind, reason string, wrapped ...error) *EventError {
	e := &EventError{Kind: kind, Reason: reason}
	if len(wrapped) > 0 {
		e.Wrapped = wrapped[0]
	}
	return e
}
