// Package pipeline wires the local durable store into a producer/consumer
// runtime providing:
//
//   - idempotent publish (republish of the same event ID is a no-op)
//   - at-least-once delivery with effect-level exactly-once semantics
//     (business effects, dedup marker and checkpoint commit in one atomic step)
//   - per-key strict ordering with bounded cross-key concurrency
//   - bounded retries with capped exponential backoff, then a terminal
//     dead-letter state carrying a distinguishable reason
//   - durable per-key checkpoints validated on restart and on seek
package pipeline

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/highcumontoa/async-event-pipeline-go/internal/event"
	"github.com/highcumontoa/async-event-pipeline-go/internal/store"
)

// Tx is the transactional business-state view passed to a Processor.
//
// Reads observe committed state plus writes made within this attempt; writes
// are buffered and become visible only when the processor returns nil, at
// which point they are committed together with the checkpoint and dedup
// marker. A failed attempt leaves no partial state behind.
type Tx interface {
	Get(key string) ([]byte, bool)
	Put(key string, value []byte)
	Delete(key string)
	// Skip marks the event as processed without business effects.
	Skip()
}

// Processor is user business logic. Return a retryable error (see Retry) to
// schedule another attempt; return a fatal error (Fatal) to dead-letter
// immediately. A plain, unclassified error is treated as retryable.
type Processor func(ctx context.Context, tx Tx, d event.Delivery) error

// Config configures a Pipeline. Zero values are replaced by sensible
// defaults in NewPipeline.
type Config struct {
	// MaxAttempts is the maximum number of processing attempts per event
	// before a retry-exhausted event dead-letters.
	MaxAttempts int
	// BackoffInitial / BackoffMax bound the exponential retry backoff.
	BackoffInitial time.Duration
	BackoffMax     time.Duration
	// Concurrency bounds how many distinct keys are processed in parallel.
	Concurrency int
	// PollInterval is how often the dispatcher scans the durable log for
	// newly published or restored events.
	PollInterval time.Duration
	// Logger receives structured decision logs (attempts, retries, commits,
	// dead-lettering). Defaults to slog.Default(); pass a discard logger in
	// benchmarks if needed.
	Logger Logger
}

// Outcome is the publish result.
type Outcome struct {
	Event event.Event
	// Dup is true when an event with the same ID already existed; no new
	// record was appended.
	Dup bool
}

// SkipResult reports an explicit forward-seek operation.
type SkipResult struct {
	Key        string
	FromSeq    uint64
	ToSeq      uint64
	SkippedIDs []string
}

var (
	ErrSeekBackwards  = errors.New("pipeline: cannot seek to an already-acknowledged position")
	ErrSeekOutOfRange = errors.New("pipeline: cannot seek beyond the known event log")
	ErrClosed         = errors.New("pipeline: closed")
	ErrRuntime        = errors.New("pipeline: runtime failure")
)

// Pipeline is the runtime facade.
type Pipeline struct {
	st   *store.Store
	proc Processor
	cfg  Config
	log  Logger

	startOnce sync.Once
	closeOnce sync.Once

	ctx    context.Context
	cancel context.CancelFunc

	wake chan struct{}

	wg sync.WaitGroup

	// runMu guards the runtime bookkeeping below (distinct from the store's
	// own lock).
	runMu    sync.Mutex
	attempts map[string]int // event ID -> attempts so far
	running  map[string]bool
	inflight int
	notifyCh chan struct{} // signaled on every durable commit
	closeErr error
}

// NewPipeline constructs a pipeline over an opened store.
func NewPipeline(s *store.Store, proc Processor, cfg Config) *Pipeline {
	return &Pipeline{
		st:       s,
		proc:     proc,
		cfg:      cfg.withDefaults(),
		log:      cfg.LoggerOrDefault(),
		attempts: make(map[string]int),
		running:  make(map[string]bool),
		wake:     make(chan struct{}, 1),
		notifyCh: make(chan struct{}, 1024),
	}
}

// Start launches the dispatcher. Delivery starts from the persisted
// checkpoints, so a restart produces results identical to a sequential run.
func (p *Pipeline) Start(ctx context.Context) error {
	p.startOnce.Do(func() {
		p.ctx, p.cancel = context.WithCancel(ctx)
		p.wg.Add(1)
		go p.dispatchLoop()
		p.signal()
	})
	return nil
}

// Close performs a graceful shutdown: it stops dispatching and waits for
// in-flight processing attempts to finish.
func (p *Pipeline) Close() error {
	p.closeOnce.Do(func() {
		if p.cancel != nil {
			p.cancel()
		}
	})
	p.wg.Wait()
	return p.closeErr
}

// Publish appends an event idempotently. A republished event ID returns the
// original event with Dup=true and never produces a second effect.
func (p *Pipeline) Publish(ctx context.Context, key, id, typ string, payload []byte) (Outcome, error) {
	if p.isClosed() {
		return Outcome{}, ErrClosed
	}
	env, dup, err := p.st.Publish(ctx, key, id, typ, payload)
	if err != nil {
		return Outcome{}, err
	}
	p.log.Info("publish accepted",
		"event_id", env.ID, "key", env.Key, "key_seq", env.KeySeq, "global_seq", env.GlobalSeq, "dup", dup)
	if !dup {
		p.signal()
	}
	return Outcome{Event: toEvent(env), Dup: dup}, nil
}

// Seek moves a key's checkpoint forward to toSeq (a per-key sequence number).
// Every event strictly between the current checkpoint and toSeq is skipped
// and recorded in an audit trail. Seeking to an already-acknowledged position
// or beyond the known log is rejected explicitly rather than silently
// continuing from an intermediate position.
func (p *Pipeline) Seek(ctx context.Context, key string, toSeq uint64) (SkipResult, error) {
	if p.isClosed() {
		return SkipResult{}, ErrClosed
	}
	cur := p.st.View().Checkpoints[key]
	if toSeq <= cur {
		err := fmt.Errorf("%w: key=%q current=%d requested=%d", ErrSeekBackwards, key, cur, toSeq)
		p.log.Error("seek rejected", "key", key, "requested", toSeq, "current", cur, "decision", "reject_backward")
		return SkipResult{}, err
	}
	if toSeq > p.st.MaxKeySeq(key) {
		err := fmt.Errorf("%w: key=%q max=%d requested=%d", ErrSeekOutOfRange, key, p.st.MaxKeySeq(key), toSeq)
		p.log.Error("seek rejected", "key", key, "requested", toSeq, "max", p.st.MaxKeySeq(key), "decision", "reject_out_of_range")
		return SkipResult{}, err
	}
	if err := p.st.Advance(ctx, key, toSeq, "seek"); err != nil {
		return SkipResult{}, err
	}
	ids := skippedIDs(p.st, key, cur, toSeq)
	p.log.Info("seek advanced",
		"key", key, "from_seq", cur, "to_seq", toSeq, "skipped", ids, "decision", "advanced")
	p.signal()
	return SkipResult{Key: key, FromSeq: cur, ToSeq: toSeq, SkippedIDs: ids}, nil
}

// DeadLetters returns terminal failure records with distinguishable reasons.
func (p *Pipeline) DeadLetters() []store.DeadRecord { return p.st.DeadLetters() }

// View returns the persisted progress/state snapshot.
func (p *Pipeline) View() store.Snapshot { return p.st.View() }

// AwaitIdle blocks until every published event is acknowledged (processed,
// skipped, or dead-lettered) or ctx is cancelled.
func (p *Pipeline) AwaitIdle(ctx context.Context) error {
	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()
	for {
		if p.idle() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		case <-p.notifyCh:
		}
	}
}

// ---- internals -------------------------------------------------------------

func (p *Pipeline) isClosed() bool {
	if p.ctx == nil {
		return false
	}
	return p.ctx.Err() != nil
}

func (p *Pipeline) signal() {
	if p.ctx == nil {
		return
	}
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

func toEvent(e store.Envelope) event.Event {
	return event.Event{ID: e.ID, Key: e.Key, Type: e.Type, Seq: e.KeySeq, Payload: e.Payload}
}

func skippedIDs(s *store.Store, key string, from, to uint64) []string {
	pending := s.Pending(key, from)
	ids := make([]string, 0, len(pending))
	for _, e := range pending {
		if e.KeySeq <= to {
			ids = append(ids, e.ID)
		}
	}
	return ids
}

// idle reports that no key has unacknowledged events and nothing is running.
func (p *Pipeline) idle() bool {
	p.runMu.Lock()
	defer p.runMu.Unlock()
	if p.inflight > 0 {
		return false
	}
	snap := p.st.View()
	for _, key := range p.st.Keys() {
		if uint64(len(p.st.Pending(key, snap.Checkpoints[key]))) > 0 {
			return false
		}
	}
	return true
}
