package pipeline

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// 事件在状态存储中的处理状态。
const (
	StatusProcessed = "processed" // 已成功处理（幂等标记存在）
	StatusDead      = "dead"      // 失败终态（死信），保留可区分原因
)

// DeadRecord 描述一条进入失败终态的事件。
type DeadRecord struct {
	Offset     int64       `json:"offset"`
	EventID    string      `json:"event_id"`
	Key        string      `json:"key"`
	Kind       FailureKind `json:"kind"`
	Reason     string      `json:"reason"`
	Attempts   int         `json:"attempts"`
	FirstError time.Time   `json:"first_error_at"`
	DeadAt     time.Time   `json:"dead_at"`
}

// ProcessRecord 是幂等记录表中的一条记录（成功或死信）。
type ProcessRecord struct {
	Offset  int64       `json:"offset"`
	EventID string      `json:"event_id"`
	Key     string      `json:"key"`
	Status  string      `json:"status"`
	Dead    *DeadRecord `json:"dead,omitempty"`
	DoneAt  time.Time   `json:"done_at"`
}

// IsDuplicate 报告该事件是否已经有终态（成功或死信）。
func (r *ProcessRecord) IsDuplicate() bool { return r != nil }

// BusinessState 是业务效果的载体：Handler 在其副本上施加效果，
// 运行时把它与幂等记录放进同一个原子快照提交，保证不留下半完成状态。
type BusinessState interface {
	// Clone 生成深拷贝，供单事件处理在副本上进行（提交失败可整体丢弃）。
	Clone() BusinessState
}

// KVState 是内置的通用业务状态：key -> 计数器。
type KVState struct {
	Values map[string]int64 `json:"values"`
}

// NewKVState 创建空的 KVState。
func NewKVState() *KVState { return &KVState{Values: make(map[string]int64)} }

func (s *KVState) Clone() BusinessState {
	if s == nil {
		return NewKVState()
	}
	cp := make(map[string]int64, len(s.Values))
	for k, v := range s.Values {
		cp[k] = v
	}
	return &KVState{Values: cp}
}

// stateEnvelope 是快照文件的 JSON 形态（业务状态以 RawMessage 延迟解码）。
type stateEnvelope struct {
	Version         int                       `json:"version"`
	CommittedOffset int64                     `json:"committed_offset"`
	Records         map[string]*ProcessRecord `json:"records"`
	Dead            []DeadRecord              `json:"dead"`
	Business        json.RawMessage           `json:"business"`
	StateVersion    uint64                    `json:"state_version"`
	UpdatedAt       time.Time                 `json:"updated_at"`
}

// StateStore 是消费状态 + 业务效果的原子持久化存储（单文件快照）。
//
// 持久化采用“临时文件 + fsync + rename + 目录 fsync”，因此对状态文件的
// 更新对崩溃是原子的：要么看到旧快照，要么看到新快照，不存在半写入状态。
// 业务效果与幂等标记在同一次 persist 中落盘，所以 Handler 的业务效果
// 永远不会在没有对应幂等标记的情况下被提交（反之亦然）。
//
// 并发提交采用乐观版本控制 version：不同业务键的 worker 并行处理，
// 提交时若基线版本过期则返回 ErrStaleState，由运行时基于最新状态重试，
// 从而保证并行提交不会互相覆盖业务效果。
type StateStore struct {
	path string

	mu        sync.Mutex
	committed int64
	records   map[string]*ProcessRecord // 事件ID -> 终态记录（幂等表）
	byOffset  map[int64]*ProcessRecord  // 偏移 -> 终态记录（水位连续推进用）
	dead      []DeadRecord
	business  BusinessState
	version   uint64
	updatedAt time.Time

	newBusiness func() BusinessState
	closed      bool
}

// OpenStateStore 打开（不存在则创建）状态快照文件。
// newBusiness 在首次创建/反序列化时生成业务状态；为 nil 时使用 *KVState。
func OpenStateStore(path string, newBusiness func() BusinessState) (*StateStore, error) {
	if newBusiness == nil {
		newBusiness = func() BusinessState { return NewKVState() }
	}
	s := &StateStore{
		path:        path,
		records:     make(map[string]*ProcessRecord),
		byOffset:    make(map[int64]*ProcessRecord),
		newBusiness: newBusiness,
	}
	if dir := filepath.Dir(path); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("pipeline: create state dir: %w", err)
		}
	}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *StateStore) load() error {
	data, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			s.business = s.newBusiness()
			return s.persistLocked()
		}
		return fmt.Errorf("pipeline: read state file: %w", err)
	}
	if len(data) == 0 {
		s.business = s.newBusiness()
		return s.persistLocked()
	}
	var env stateEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		return fmt.Errorf("%w: state snapshot not json: %v", ErrLogCorrupt, err)
	}
	if env.Version != 1 {
		return fmt.Errorf("pipeline: unsupported state snapshot version %d", env.Version)
	}
	if env.Records == nil {
		env.Records = make(map[string]*ProcessRecord)
	}
	// 结构校验：记录必须完整可索引。
	for id, r := range env.Records {
		if r == nil || r.EventID == "" {
			return fmt.Errorf("%w: malformed record for %q", ErrLogCorrupt, id)
		}
	}
	s.committed = env.CommittedOffset
	s.records = env.Records
	s.byOffset = make(map[int64]*ProcessRecord, len(s.records))
	for id, r := range s.records {
		// 不同业务键并行提交时，offset >= committed 的终态记录是正常状态
		// （只等前方空洞补齐后水位即可连续跨过它）；同一 offset 不得冲突。
		if cur, dup := s.byOffset[r.Offset]; dup && cur.EventID != r.EventID {
			return fmt.Errorf("%w: two records at offset=%d: %q vs %q",
				ErrLogCorrupt, r.Offset, cur.EventID, id)
		}
		s.byOffset[r.Offset] = s.records[id]
	}
	// 恢复后从持久化水位起连续推进：若崩溃前恰好已写记录但水位推进未单独
	// 落盘（水位推进总是与记录同一快照，此循环通常为空），此处兜底收敛。
	for {
		if _, ok := s.byOffset[s.committed]; !ok {
			break
		}
		s.committed++
	}
	s.dead = env.Dead
	s.updatedAt = env.UpdatedAt
	s.version = env.StateVersion
	s.business = s.newBusiness()
	if len(env.Business) > 0 {
		if err := json.Unmarshal(env.Business, s.business); err != nil {
			return fmt.Errorf("pipeline: decode business state: %w", err)
		}
	}
	return nil
}

// Snapshot 返回当前状态的深拷贝。
func (s *StateStore) Snapshot() StateSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return StateSnapshot{
		CommittedOffset: s.committed,
		Records:         cloneRecords(s.records),
		Dead:            append([]DeadRecord(nil), s.dead...),
		Business:        s.business.Clone(),
		StateVersion:    s.version,
		UpdatedAt:       s.updatedAt,
	}
}

func cloneRecords(in map[string]*ProcessRecord) map[string]*ProcessRecord {
	out := make(map[string]*ProcessRecord, len(in))
	for k, v := range in {
		cp := *v
		if v.Dead != nil {
			d := *v.Dead
			cp.Dead = &d
		}
		out[k] = &cp
	}
	return out
}

// StateSnapshot 是持久化状态的只读快照。
type StateSnapshot struct {
	CommittedOffset int64                     `json:"committed_offset"`
	Records         map[string]*ProcessRecord `json:"records"`
	Dead            []DeadRecord              `json:"dead"`
	Business        BusinessState             `json:"-"`
	StateVersion    uint64                    `json:"state_version"`
	UpdatedAt       time.Time                 `json:"updated_at"`
}

// Committed 返回已提交水位（已完成终态的最大连续偏移+1）。
func (s *StateStore) Committed() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.committed
}

// BusinessAndVersion 返回当前业务状态的深拷贝与版本号。
func (s *StateStore) BusinessAndVersion() (BusinessState, uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.business.Clone(), s.version
}

// Business 返回当前业务状态的深拷贝。
func (s *StateStore) Business() BusinessState {
	b, _ := s.BusinessAndVersion()
	return b
}

// Lookup 按事件 ID 查询终态记录；未处理返回 nil,false。
func (s *StateStore) Lookup(eventID string) (*ProcessRecord, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.records[eventID]
	if !ok {
		return nil, false
	}
	cp := *r
	if r.Dead != nil {
		d := *r.Dead
		cp.Dead = &d
	}
	return &cp, true
}

// DeadLetters 返回死信列表的拷贝。
func (s *StateStore) DeadLetters() []DeadRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]DeadRecord(nil), s.dead...)
}

// CommitResult 描述一次终态提交的结果。
type CommitResult struct {
	Record       *ProcessRecord
	Duplicate    bool   // 事件 ID 已有终态（重复投递，效果未再次生效）
	StateVersion uint64 // 提交后的新版本（Duplicate 时不变）
	NewCommitted int64  // 本次提交连带推进到的水位（可能不变，说明前方还有空洞）
	Advanced     bool   // 水位是否发生推进
}

// CommitProcessed 以乐观并发控制原子提交：幂等记录 + 业务效果同快照落盘。
//
//   - baseVersion：worker 运行 Handle 时所基于的业务状态版本；过期返回
//     ErrStaleState，运行时基于最新状态重试 Handle。
//   - 事件 ID 已有终态（并发竞争下另一 worker 已提交，或同 ID 的更早
//     投递已生效）：业务效果不二次生效，但会在本次投递的 offset 处登记
//     重复标记并连续推进水位，返回 Duplicate=true。
//   - 记录先写入（offset 可以超过当前水位，不同业务键并行必然如此）；
//     随后仅当该事件恰好位于水位边界时，连续推进水位，绝不越过空洞。
func (s *StateStore) CommitProcessed(rec ProcessRecord, business BusinessState, baseVersion uint64) (CommitResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.records[rec.EventID]; ok {
		// 并发重复：另一执行已持有该 ID 的终态。效果绝不二次生效；
		// 但本次投递在自己的 offset 上仍要登记，供连续水位跨过它。
		advanced := false
		if _, existsOffset := s.byOffset[rec.Offset]; !existsOffset {
			s.byOffset[rec.Offset] = existing
			advanced = s.advanceWatermarkLocked()
			if err := s.persistLocked(); err != nil {
				return CommitResult{}, err
			}
		}
		cp := *existing
		return CommitResult{
			Record:       &cp,
			Duplicate:    true,
			StateVersion: s.version,
			NewCommitted: s.committed,
			Advanced:     advanced,
		}, nil
	}
	if baseVersion != s.version {
		return CommitResult{StateVersion: s.version}, ErrStaleState
	}
	rec.Status = StatusProcessed
	rec.DoneAt = time.Now()
	stored := rec
	s.records[rec.EventID] = &stored
	s.byOffset[rec.Offset] = &stored
	s.business = business.Clone()
	s.version++
	if err := s.persistLocked(); err != nil {
		// persist 失败：回滚内存变更，交给重新处理，避免“内存有、磁盘无”。
		delete(s.records, rec.EventID)
		delete(s.byOffset, rec.Offset)
		s.version--
		return CommitResult{}, err
	}
	advanced := s.advanceWatermarkLocked()
	cp := stored
	return CommitResult{
		Record:       &cp,
		StateVersion: s.version,
		NewCommitted: s.committed,
		Advanced:     advanced,
	}, nil
}

// CommitDead 以乐观并发控制原子提交死信终态。
// 业务状态保持为 baseVersion 对应的最新快照（失败的 Handle 只在克隆上
// 运行，效果被整体丢弃，因此提交的 business 即“未施加本事件”的状态）。
func (s *StateStore) CommitDead(dead DeadRecord, business BusinessState, baseVersion uint64) (CommitResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.records[dead.EventID]; ok {
		// 并发重复：沿用已有终态，不重复计入死信列表；本 offset 仍需登记。
		advanced := false
		if _, existsOffset := s.byOffset[dead.Offset]; !existsOffset {
			s.byOffset[dead.Offset] = existing
			advanced = s.advanceWatermarkLocked()
			if err := s.persistLocked(); err != nil {
				return CommitResult{}, err
			}
		}
		cp := *existing
		return CommitResult{
			Record:       &cp,
			Duplicate:    true,
			StateVersion: s.version,
			NewCommitted: s.committed,
			Advanced:     advanced,
		}, nil
	}
	if baseVersion != s.version {
		return CommitResult{StateVersion: s.version}, ErrStaleState
	}
	dead.DeadAt = time.Now()
	s.dead = append(s.dead, dead)
	rec := &ProcessRecord{
		Offset:  dead.Offset,
		EventID: dead.EventID,
		Key:     dead.Key,
		Status:  StatusDead,
		Dead:    &dead,
		DoneAt:  dead.DeadAt,
	}
	s.records[dead.EventID] = rec
	s.byOffset[dead.Offset] = rec
	s.business = business.Clone()
	s.version++
	if err := s.persistLocked(); err != nil {
		s.dead = s.dead[:len(s.dead)-1]
		delete(s.records, dead.EventID)
		delete(s.byOffset, dead.Offset)
		s.version--
		return CommitResult{}, err
	}
	advanced := s.advanceWatermarkLocked()
	cp := *rec
	return CommitResult{
		Record:       &cp,
		StateVersion: s.version,
		NewCommitted: s.committed,
		Advanced:     advanced,
	}, nil
}

// MarkDuplicate 为重复投递事件在 offset 索引处登记其已有终态记录，并尝试
// 连续推进水位。它不改变幂等表（byEventID）与业务效果：重复事件的业务
// 效果绝不第二次生效。若该 offset 处已有记录（其他路径抢先登记），直接
// 尝试推进水位即可。
func (s *StateStore) MarkDuplicate(offset int64, existing *ProcessRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cur, ok := s.byOffset[offset]; !ok {
		// 登记一条指向既有终态记录的引用；同一事件 ID 的效果已在原记录中。
		s.byOffset[offset] = existing
		s.advanceWatermarkLocked()
		return s.persistLocked()
	} else if cur.Offset != offset {
		return fmt.Errorf("%w: offset index inconsistent at %d", ErrLogCorrupt, offset)
	}
	s.advanceWatermarkLocked()
	return nil
}

// advanceWatermarkLocked 从当前水位起，只要对应 offset 已有终态记录就
// 连续推进；遇到未完成 offset（空洞）立即停止。返回是否发生推进。
func (s *StateStore) advanceWatermarkLocked() bool {
	start := s.committed
	for {
		if _, ok := s.byOffset[s.committed]; !ok {
			break
		}
		s.committed++
	}
	return s.committed > start
}

// AdvanceCommitted 供“重复投递”路径使用：重复事件本身已是终态记录，
// 只需尝试让水位跨过它（实际推进由 advanceWatermarkLocked 的连续规则决定）。
// 目标早于当前水位会被拒绝（ErrRollbackRejected）；目标前方存在尚未完成
// 的空洞时同样拒绝，避免从中间静默继续。
func (s *StateStore) AdvanceCommitted(offset int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if offset < s.committed {
		return fmt.Errorf("%w: current=%d requested=%d",
			ErrRollbackRejected, s.committed, offset)
	}
	if offset == s.committed {
		return nil
	}
	for want := s.committed; want < offset; want++ {
		if _, ok := s.byOffset[want]; !ok {
			return fmt.Errorf("%w: gap at offset=%d while advancing to %d",
				ErrOffsetBeyondEnd, want, offset)
		}
	}
	s.committed = offset
	return s.persistLocked()
}

// ResetForReplay 丢弃全部消费进度并回到 offset（当前实现要求 offset=0，
// 即从日志最早可恢复位置重放），供 Pipeline.Replay 在停机状态下显式调用。
//
// 为保证重放结果与“首次顺序执行”严格一致，水位、幂等/死信记录与业务
// 效果三者必须同时回到初始状态；运行时随后从日志头顺序重放全部事件，
// 由幂等机制收敛出与原来一致（或按当前 Handler 语义重新判定）的结果。
// 拒绝回退到 0 之前（不可恢复位置）的请求，而不是从中间静默继续。
func (s *StateStore) ResetForReplay(offset int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if offset < 0 {
		return fmt.Errorf("%w: replay offset=%d", ErrOffsetBeforeEarliest, offset)
	}
	if offset != 0 {
		return fmt.Errorf("%w: replay to mid-stream offset=%d is not recoverable without a snapshot boundary",
			ErrOffsetBeforeEarliest, offset)
	}
	s.committed = 0
	s.records = make(map[string]*ProcessRecord)
	s.byOffset = make(map[int64]*ProcessRecord)
	s.dead = nil
	s.business = s.newBusiness()
	s.version++
	return s.persistLocked()
}

// persistLocked 以原子替换方式落盘整个快照（调用方必须持有 mu）。
func (s *StateStore) persistLocked() error {
	if s.closed {
		return ErrPipelineClosed
	}
	bizRaw, err := json.Marshal(s.business)
	if err != nil {
		return fmt.Errorf("pipeline: marshal business state: %w", err)
	}
	env := stateEnvelope{
		Version:         1,
		CommittedOffset: s.committed,
		Records:         s.records,
		Dead:            s.dead,
		Business:        bizRaw,
		StateVersion:    s.version,
		UpdatedAt:       time.Now(),
	}
	data, err := json.MarshalIndent(&env, "", "  ")
	if err != nil {
		return fmt.Errorf("pipeline: marshal state: %w", err)
	}
	dir := filepath.Dir(s.path)
	tmp, err := os.CreateTemp(dir, ".state-*.tmp")
	if err != nil {
		return fmt.Errorf("pipeline: create state tmp: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // rename 成功后 Remove 无害
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("pipeline: write state tmp: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("pipeline: fsync state tmp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("pipeline: close state tmp: %w", err)
	}
	if err := os.Rename(tmpName, s.path); err != nil {
		return fmt.Errorf("pipeline: rename state: %w", err)
	}
	if df, err := os.Open(dir); err == nil {
		_ = df.Sync()
		_ = df.Close()
	}
	s.updatedAt = env.UpdatedAt
	return nil
}

// Close 释放存储资源。
func (s *StateStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}
