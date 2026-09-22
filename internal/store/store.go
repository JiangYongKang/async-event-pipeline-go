// Package store is the on-disk, single-process durable backing store for the
// pipeline. It uses only the local filesystem (no external middleware):
//
//   - events.jsonl: append-only event log (WAL)
//   - state.json:   committed consumer state (checkpoints, dedup, business KV)
//   - dead.jsonl:   dead-letter records with terminal failure reasons
//
// All effect visibility is tied to a single state commit, so there is no
// window where a business effect is visible without its checkpoint, or vice
// versa: state.json is replaced atomically (temp file + fsync + rename).
package store

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// Envelope is one record in the event log.
type Envelope struct {
	GlobalSeq uint64 `json:"g"`
	Key       string `json:"k"`
	KeySeq    uint64 `json:"s"`
	ID        string `json:"id"`
	Type      string `json:"t"`
	Payload   []byte `json:"p,omitempty"`
}

// DeadRecord describes an event that reached a terminal failure state.
// Reason carries a domain-distinguishable code; Fatal distinguishes an
// immediately fatal business rejection from retry-exhaustion.
type DeadRecord struct {
	Event    Envelope `json:"event"`
	Attempts int      `json:"attempts"`
	Reason   string   `json:"reason"`
	Fatal    bool     `json:"fatal"`
	At       string   `json:"at"`
}

// Mutation is a proposed atomic state transition produced inside a consumer
// transaction. It is applied only if event processing succeeds.
type Mutation struct {
	Puts    map[string][]byte
	Deletes []string
	// Skip advances the key checkpoint without producing business effects
	// (e.g. an explicitly skipped command).
	Skip bool
}

// Snapshot is an observable, point-in-time view of consumer progress.
type Snapshot struct {
	Checkpoints map[string]uint64
	Processed   map[string]map[string]bool
	State       map[string]string
	GlobalSeq   uint64
}

// ErrNotFound is returned by Get for unknown business-state keys.
var ErrNotFound = errors.New("store: not found")

const (
	eventsFile = "events.jsonl"
	stateFile  = "state.json"
	deadFile   = "dead.jsonl"
)

type persistedState struct {
	Version     int                 `json:"version"`
	GlobalSeq   uint64              `json:"global_seq"`
	Checkpoints map[string]uint64   `json:"ckpt"`
	Processed   map[string][]string `json:"proc"`
	State       map[string]string   `json:"state"`
	Skipped     []SkippedAudit      `json:"skipped,omitempty"`
	Dead        []DeadRecord        `json:"dead,omitempty"`
}

// SkippedAudit records one explicit checkpoint advance past events.
type SkippedAudit struct {
	Key   string    `json:"key"`
	ToSeq uint64    `json:"to_seq"`
	IDs   []string  `json:"ids"`
	At    time.Time `json:"at"`
}

// Store is the durable local store. Methods are safe for concurrent use.
type Store struct {
	dir string

	mu        sync.Mutex
	events    []Envelope
	byID      map[string]Envelope
	keyIdx    map[string][]Envelope // per-key envelopes in KeySeq order
	globalSeq uint64

	checkpoints map[string]uint64
	processed   map[string]map[string]bool
	state       map[string]string
	skipped     []SkippedAudit
	dead        []DeadRecord

	eventsF *os.File
	deadF   *os.File

	closeOnce sync.Once
	closeErr  error
}

// Open opens (or creates) a store rooted at dir.
func Open(ctx context.Context, dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("store: create dir: %w", err)
	}
	s := &Store{
		dir:         dir,
		byID:        make(map[string]Envelope),
		keyIdx:      make(map[string][]Envelope),
		checkpoints: make(map[string]uint64),
		processed:   make(map[string]map[string]bool),
		state:       make(map[string]string),
	}

	if err := s.loadStateLocked(); err != nil {
		return nil, err
	}
	if err := s.replayEventsLocked(); err != nil {
		return nil, err
	}
	if err := s.openAppendFilesLocked(); err != nil {
		return nil, err
	}
	if err := s.reconcileDeadProjectionLocked(); err != nil {
		return nil, err
	}
	return s, nil
}

// Close flushes and releases the store. It is safe to call multiple times.
func (s *Store) Close() error {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		var err error
		if s.eventsF != nil {
			if e := s.eventsF.Sync(); e != nil {
				err = errors.Join(err, e)
			}
			if e := s.eventsF.Close(); e != nil {
				err = errors.Join(err, e)
			}
			s.eventsF = nil
		}
		if s.deadF != nil {
			if e := s.deadF.Sync(); e != nil {
				err = errors.Join(err, e)
			}
			if e := s.deadF.Close(); e != nil {
				err = errors.Join(err, e)
			}
			s.deadF = nil
		}
		s.closeErr = err
	})
	return s.closeErr
}

// Dir reports the on-disk root directory.
func (s *Store) Dir() string { return s.dir }

// Publish appends a new event, or returns the existing envelope for a
// republished event ID (idempotent publish). The second return value reports
// whether an existing record was returned (dup=true).
func (s *Store) Publish(ctx context.Context, key, id, typ string, payload []byte) (Envelope, bool, error) {
	if id == "" {
		return Envelope{}, false, errors.New("store: event id is required")
	}
	if key == "" {
		return Envelope{}, false, errors.New("store: event key is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if env, ok := s.byID[id]; ok {
		return env, true, nil
	}
	s.globalSeq++
	keySeq := uint64(len(s.keyIdx[key])) + 1
	env := Envelope{
		GlobalSeq: s.globalSeq,
		Key:       key,
		KeySeq:    keySeq,
		ID:        id,
		Type:      typ,
		Payload:   append([]byte(nil), payload...),
	}
	if err := s.writeEventLocked(env); err != nil {
		// Roll back the in-memory reservation so the failed append did not
		// consume sequence numbers, and remove any partial line so the WAL
		// stays replayable.
		s.globalSeq--
		return Envelope{}, false, err
	}
	s.events = append(s.events, env)
	s.byID[id] = env
	s.keyIdx[key] = append(s.keyIdx[key], env)
	return env, false, nil
}

// Pending returns events for key with KeySeq strictly greater than afterSeq,
// in key order.
func (s *Store) Pending(key string, afterSeq uint64) []Envelope {
	s.mu.Lock()
	defer s.mu.Unlock()
	src := s.keyIdx[key]
	out := make([]Envelope, 0, len(src))
	for _, e := range src {
		if e.KeySeq > afterSeq {
			out = append(out, e)
		}
	}
	return out
}

// Keys returns all known ordering keys in sorted order.
func (s *Store) Keys() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.keyIdx))
	for k := range s.keyIdx {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// MaxKeySeq returns the highest assigned KeySeq for key (0 if unknown).
func (s *Store) MaxKeySeq(key string) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return uint64(len(s.keyIdx[key]))
}

// Get reads a committed business-state key.
func (s *Store) Get(k string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.state[k]
	if !ok {
		return nil, ErrNotFound
	}
	return []byte(v), nil
}

// Commit atomically applies the processing outcome of exactly one event:
// mutation, dedup marker, checkpoint advance and optional dead-letter record.
// Either the full outcome is durable or none of it is — in-memory state is
// replaced only after the atomic state-file write succeeds.
func (s *Store) Commit(ctx context.Context, env Envelope, mut Mutation, dead *DeadRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	ckpt := s.checkpoints[env.Key]
	if env.KeySeq != ckpt+1 {
		return fmt.Errorf("store: out-of-order commit for key %q: checkpoint=%d event=%d", env.Key, ckpt, env.KeySeq)
	}

	// Build the next state on copies first; nothing is observable until the
	// atomic write succeeds.
	newState := make(map[string]string, len(s.state)+len(mut.Puts))
	for k, v := range s.state {
		newState[k] = v
	}
	for _, del := range mut.Deletes {
		delete(newState, del)
	}
	for k, v := range mut.Puts {
		newState[k] = string(v)
	}
	newProcessed := make(map[string]map[string]bool, len(s.processed)+1)
	for k, set := range s.processed {
		cp := make(map[string]bool, len(set))
		for id, v := range set {
			cp[id] = v
		}
		newProcessed[k] = cp
	}
	if newProcessed[env.Key] == nil {
		newProcessed[env.Key] = make(map[string]bool)
	}
	newProcessed[env.Key][env.ID] = true

	newCheckpoints := make(map[string]uint64, len(s.checkpoints)+1)
	for k, v := range s.checkpoints {
		newCheckpoints[k] = v
	}
	newCheckpoints[env.Key] = env.KeySeq

	newDead := s.dead
	if dead != nil {
		dead.Event = env
		if dead.At == "" {
			dead.At = time.Now().UTC().Format(time.RFC3339Nano)
		}
		newDead = make([]DeadRecord, len(s.dead), len(s.dead)+1)
		copy(newDead, s.dead)
		newDead = append(newDead, *dead)
	}

	ps := s.buildPersistedLocked(newState, newProcessed, newCheckpoints, s.skipped, newDead)
	if err := s.writeStateFileLocked(ps); err != nil {
		return err
	}

	// Durable: publish the outcome to in-memory readers.
	s.state = newState
	s.processed = newProcessed
	s.checkpoints = newCheckpoints
	s.dead = newDead

	if dead != nil {
		// Best-effort projection; state.json remains authoritative and the
		// projection is reconciled on the next Open if this is missed.
		_ = s.writeDeadLocked(*dead)
	}
	return nil
}

// Advance moves the checkpoint of key to seq without business effects. Every
// event strictly between the current checkpoint and seq is recorded in the
// audit trail. Backward moves are rejected by the pipeline layer; here we
// additionally guard against non-forward movement.
func (s *Store) Advance(ctx context.Context, key string, seq uint64, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur := s.checkpoints[key]
	if seq <= cur {
		return fmt.Errorf("%w: key=%q current=%d requested=%d", ErrSeekBackwards, key, cur, seq)
	}
	max := uint64(len(s.keyIdx[key]))
	if seq > max {
		return fmt.Errorf("%w: key=%q max=%d requested=%d", ErrSeekOutOfRange, key, max, seq)
	}
	skippedIDs := make([]string, 0, seq-cur)
	for _, e := range s.keyIdx[key][cur:seq] {
		skippedIDs = append(skippedIDs, e.ID)
		if s.processed[key] == nil {
			s.processed[key] = make(map[string]bool)
		}
		s.processed[key][e.ID] = true
	}
	newSkipped := append(append([]SkippedAudit(nil), s.skipped...), SkippedAudit{
		Key:   key,
		ToSeq: seq,
		IDs:   skippedIDs,
		At:    time.Now().UTC(),
	})
	newCheckpoints := make(map[string]uint64, len(s.checkpoints)+1)
	for k, v := range s.checkpoints {
		newCheckpoints[k] = v
	}
	newCheckpoints[key] = seq
	newProcessed := make(map[string]map[string]bool, len(s.processed)+1)
	for k, set := range s.processed {
		cp := make(map[string]bool, len(set))
		for id, v := range set {
			cp[id] = v
		}
		newProcessed[k] = cp
	}
	if newProcessed[key] == nil {
		newProcessed[key] = make(map[string]bool)
	}
	for _, e := range s.keyIdx[key][cur:seq] {
		newProcessed[key][e.ID] = true
	}
	ps := s.buildPersistedLocked(s.state, newProcessed, newCheckpoints, newSkipped, s.dead)
	if err := s.writeStateFileLocked(ps); err != nil {
		return err
	}
	s.processed = newProcessed
	s.checkpoints = newCheckpoints
	s.skipped = newSkipped
	return nil
}

// DeadLetters returns all terminal-failure records in arrival order. The
// authoritative source is the atomically committed state; dead.jsonl is only a
// human-readable projection rebuilt on Open.
func (s *Store) DeadLetters() []DeadRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]DeadRecord, len(s.dead))
	copy(out, s.dead)
	return out
}

// View returns a consistent point-in-time snapshot.
func (s *Store) View() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	snap := Snapshot{
		Checkpoints: make(map[string]uint64, len(s.checkpoints)),
		Processed:   make(map[string]map[string]bool, len(s.processed)),
		State:       make(map[string]string, len(s.state)),
		GlobalSeq:   s.globalSeq,
	}
	for k, v := range s.checkpoints {
		snap.Checkpoints[k] = v
	}
	for k, set := range s.processed {
		cp := make(map[string]bool, len(set))
		for id, v := range set {
			cp[id] = v
		}
		snap.Processed[k] = cp
	}
	for k, v := range s.state {
		snap.State[k] = v
	}
	return snap
}

// ---- internals -------------------------------------------------------------

// ErrSeekBackwards / ErrSeekOutOfRange are exported sentinels for the
// pipeline's seek validation and for callers that use the store directly.
var (
	ErrSeekBackwards  = errors.New("store: seek to an already-acknowledged position")
	ErrSeekOutOfRange = errors.New("store: seek beyond the known event log")
)

func (s *Store) loadStateLocked() error {
	path := filepath.Join(s.dir, stateFile)
	b, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("store: read state: %w", err)
	}
	if len(b) == 0 {
		return nil
	}
	var ps persistedState
	if err := json.Unmarshal(b, &ps); err != nil {
		return fmt.Errorf("store: corrupt state file: %w", err)
	}
	s.globalSeq = ps.GlobalSeq
	s.checkpoints = ps.Checkpoints
	if s.checkpoints == nil {
		s.checkpoints = make(map[string]uint64)
	}
	s.state = ps.State
	if s.state == nil {
		s.state = make(map[string]string)
	}
	s.processed = make(map[string]map[string]bool)
	for k, ids := range ps.Processed {
		set := make(map[string]bool, len(ids))
		for _, id := range ids {
			set[id] = true
		}
		s.processed[k] = set
	}
	s.skipped = ps.Skipped
	s.dead = ps.Dead
	return nil
}

func (s *Store) replayEventsLocked() error {
	path := filepath.Join(s.dir, eventsFile)
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("store: open event log: %w", err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1024*1024), 16*1024*1024)
	var maxGlobal uint64
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var env Envelope
		if err := json.Unmarshal(line, &env); err != nil {
			return fmt.Errorf("store: corrupt event log: %w", err)
		}
		s.events = append(s.events, env)
		s.byID[env.ID] = env
		s.keyIdx[env.Key] = append(s.keyIdx[env.Key], env)
		if env.GlobalSeq > maxGlobal {
			maxGlobal = env.GlobalSeq
		}
	}
	if err := sc.Err(); err != nil {
		return fmt.Errorf("store: scan event log: %w", err)
	}
	// The WAL is authoritative for global sequence allocation.
	if maxGlobal > s.globalSeq {
		s.globalSeq = maxGlobal
	}
	return nil
}

func (s *Store) openAppendFilesLocked() error {
	ef, err := os.OpenFile(filepath.Join(s.dir, eventsFile), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("store: open event log for append: %w", err)
	}
	s.eventsF = ef
	df, err := os.OpenFile(filepath.Join(s.dir, deadFile), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		_ = ef.Close()
		s.eventsF = nil
		return fmt.Errorf("store: open dead-letter log: %w", err)
	}
	s.deadF = df
	return nil
}

func (s *Store) writeEventLocked(env Envelope) error {
	b, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("store: marshal event: %w", err)
	}
	b = append(b, '\n')
	off, err := s.eventsF.Seek(0, 2) // current size (O_APPEND writes go here)
	if err != nil {
		return fmt.Errorf("store: stat event log: %w", err)
	}
	if _, err := s.eventsF.Write(b); err != nil {
		_ = s.eventsF.Truncate(off)
		return fmt.Errorf("store: append event: %w", err)
	}
	if err := s.eventsF.Sync(); err != nil {
		_ = s.eventsF.Truncate(off)
		return fmt.Errorf("store: sync event log: %w", err)
	}
	return nil
}

func (s *Store) writeDeadLocked(rec DeadRecord) error {
	b, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("store: marshal dead record: %w", err)
	}
	b = append(b, '\n')
	if _, err := s.deadF.Write(b); err != nil {
		return fmt.Errorf("store: append dead record: %w", err)
	}
	if err := s.deadF.Sync(); err != nil {
		return fmt.Errorf("store: sync dead log: %w", err)
	}
	return nil
}

// reconcileDeadProjectionLocked rebuilds the human-readable dead.jsonl from
// the authoritative state, repairing any projection writes missed by a crash
// (the append may have been skipped if the process died right after the
// atomic state rename).
func (s *Store) reconcileDeadProjectionLocked() error {
	if len(s.dead) == 0 {
		return nil
	}
	if s.deadF != nil {
		_ = s.deadF.Close()
		s.deadF = nil
	}
	path := filepath.Join(s.dir, deadFile)
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("store: rebuild dead projection: %w", err)
	}
	w := bufio.NewWriter(f)
	for _, rec := range s.dead {
		b, err := json.Marshal(rec)
		if err != nil {
			_ = f.Close()
			_ = os.Remove(tmp)
			return fmt.Errorf("store: marshal dead record: %w", err)
		}
		if _, err := w.Write(append(b, '\n')); err != nil {
			_ = f.Close()
			_ = os.Remove(tmp)
			return err
		}
	}
	if err := w.Flush(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("store: finalize dead projection: %w", err)
	}
	df, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("store: reopen dead projection: %w", err)
	}
	s.deadF = df
	return nil
}

// buildPersistedLocked assembles the serializable state view.
func (s *Store) buildPersistedLocked(
	state map[string]string,
	processed map[string]map[string]bool,
	checkpoints map[string]uint64,
	skipped []SkippedAudit,
	dead []DeadRecord,
) persistedState {
	ps := persistedState{
		Version:     1,
		GlobalSeq:   s.globalSeq,
		Checkpoints: checkpoints,
		State:       state,
		Skipped:     skipped,
		Dead:        dead,
	}
	ps.Processed = make(map[string][]string, len(processed))
	for k, set := range processed {
		ids := make([]string, 0, len(set))
		for id := range set {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		ps.Processed[k] = ids
	}
	return ps
}

// writeStateFileLocked atomically replaces state.json: write temp in the same
// directory, fsync, then rename. The rename is atomic on local filesystems,
// so a crash can never leave a half-written state file.
func (s *Store) writeStateFileLocked(ps persistedState) error {
	b, err := json.MarshalIndent(&ps, "", "  ")
	if err != nil {
		return fmt.Errorf("store: marshal state: %w", err)
	}
	tmp := filepath.Join(s.dir, stateFile+".tmp")
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("store: create temp state: %w", err)
	}
	if _, err := f.Write(b); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("store: write temp state: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("store: sync temp state: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("store: close temp state: %w", err)
	}
	if err := os.Rename(tmp, filepath.Join(s.dir, stateFile)); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("store: rename state: %w", err)
	}
	return nil
}
