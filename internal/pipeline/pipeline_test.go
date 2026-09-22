package pipeline_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/highcumontoa/async-event-pipeline-go/internal/event"
	"github.com/highcumontoa/async-event-pipeline-go/internal/pipeline"
	"github.com/highcumontoa/async-event-pipeline-go/internal/store"
)

func testLogger(t *testing.T) pipeline.Logger {
	t.Helper()
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func waitIdle(t *testing.T, p *pipeline.Pipeline, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := p.AwaitIdle(ctx); err != nil {
		t.Fatalf("pipeline did not become idle: %v", err)
	}
}

func newPipeline(t *testing.T, proc pipeline.Processor, cfg pipeline.Config) (*pipeline.Pipeline, string) {
	t.Helper()
	p, st, _ := newPipelineState(t, proc, cfg, true)
	return p, st.Dir()
}

// newPipelineStopped builds a runtime with persisted events but does not
// start the dispatcher, so a test can exercise seek-before-consume semantics.
func newPipelineStopped(t *testing.T, proc pipeline.Processor, cfg pipeline.Config) (*pipeline.Pipeline, *store.Store) {
	t.Helper()
	p, st, _ := newPipelineState(t, proc, cfg, false)
	return p, st
}

func newPipelineState(t *testing.T, proc pipeline.Processor, cfg pipeline.Config, autostart bool) (*pipeline.Pipeline, *store.Store, string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "data")
	if cfg.Logger == nil {
		cfg.Logger = testLogger(t)
	}
	st, err := store.Open(context.Background(), dir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	p := pipeline.NewPipeline(st, proc, cfg)
	if autostart {
		if err := p.Start(context.Background()); err != nil {
			t.Fatalf("start: %v", err)
		}
	}
	t.Cleanup(func() {
		_ = p.Close()
		_ = st.Close()
	})
	return p, st, dir
}

func TestDuplicatePublishAppliesEffectOnce(t *testing.T) {
	var applies int64
	var mu sync.Mutex
	seen := map[string]int{}
	proc := func(ctx context.Context, tx pipeline.Tx, d event.Delivery) error {
		mu.Lock()
		defer mu.Unlock()
		seen[d.Event.ID]++
		v, _ := tx.Get("counter")
		var n int
		if v != nil {
			for _, c := range string(v) {
				n = n*10 + int(c-'0')
			}
		}
		n++
		atomic.AddInt64(&applies, 1)
		tx.Put("counter", []byte(itoa(n)))
		return nil
	}
	p, _ := newPipeline(t, proc, pipeline.Config{})

	ctx := context.Background()
	o1, err := p.Publish(ctx, "k1", "DUP-1", "inc", nil)
	if err != nil || o1.Dup {
		t.Fatalf("first publish: %+v %v", o1, err)
	}
	// Concurrently and repeatedly republish the same ID.
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			o, err := p.Publish(ctx, "k1", "DUP-1", "inc", nil)
			if err != nil || !o.Dup {
				t.Errorf("republish: dup=%v err=%v", o.Dup, err)
			}
		}()
	}
	wg.Wait()
	waitIdle(t, p, 5*time.Second)

	if atomic.LoadInt64(&applies) != 1 {
		t.Fatalf("effect applied %d times, want exactly 1; seen=%v", applies, seen)
	}
	if v, ok := p.View().State["counter"]; !ok || v != "1" {
		t.Fatalf("counter = %q (ok=%v), want 1", v, ok)
	}
	t.Logf("判定依据: event_id=DUP-1 处理次数=%d, counter=%s, 重复发布均返回 dup=true", seen["DUP-1"], p.View().State["counter"])
}

var retryTestPipeline *pipeline.Pipeline

func TestRetrySucceedsAndFatalDeadLetters(t *testing.T) {
	var mu sync.Mutex
	attempts := map[string]int{}
	proc := func(ctx context.Context, tx pipeline.Tx, d event.Delivery) error {
		mu.Lock()
		attempts[d.Event.ID] = d.Attempt
		progress := uint64(0)
		if retryTestPipeline != nil {
			progress = retryTestPipeline.View().Checkpoints[d.Event.Key]
		}
		mu.Unlock()
		t.Logf("event_id=%s attempt=%d progress=%d decision=evaluate", d.Event.ID, d.Attempt, progress)
		switch d.Event.ID {
		case "FLAKY":
			if d.Attempt < 3 {
				return pipeline.Retryf("TRANSIENT: attempt %d failing", d.Attempt)
			}
			tx.Put("flaky-done", []byte("yes"))
			return nil
		case "POISON":
			return pipeline.Fatalf("BAD_COMMAND: cannot parse")
		}
		return nil
	}
	p, _ := newPipeline(t, proc, pipeline.Config{MaxAttempts: 5, BackoffInitial: time.Millisecond})
	retryTestPipeline = p

	ctx := context.Background()
	if _, err := p.Publish(ctx, "k", "FLAKY", "work", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Publish(ctx, "k", "POISON", "bad", nil); err != nil {
		t.Fatal(err)
	}
	// An event after the poison event proves dead-lettering does not block
	// later events on the same key.
	if _, err := p.Publish(ctx, "k", "AFTER", "work", nil); err != nil {
		t.Fatal(err)
	}
	waitIdle(t, p, 5*time.Second)

	mu.Lock()
	if attempts["FLAKY"] != 3 {
		t.Fatalf("FLAKY attempts = %d, want 3 (2 retries then success)", attempts["FLAKY"])
	}
	mu.Unlock()
	if got := p.View().State["flaky-done"]; got != "yes" {
		t.Fatalf("flaky event side effect missing: %q", got)
	}

	dl := p.DeadLetters()
	if len(dl) != 1 || dl[0].Event.ID != "POISON" || dl[0].Fatal != true {
		t.Fatalf("dead letters wrong: %+v", dl)
	}
	if dl[0].Reason == "" || !contains(dl[0].Reason, "BAD_COMMAND") {
		t.Fatalf("dead-letter reason not distinguishable: %q", dl[0].Reason)
	}
	if p.View().Checkpoints["k"] != 3 {
		t.Fatalf("checkpoint = %d, want 3 (success + dead-letter + success all acked)", p.View().Checkpoints["k"])
	}
	t.Logf("判定依据: FLAKY 总尝试=%d 最终=committed; POISON reason=%s fatal=%v; checkpoint=k:3",
		attempts["FLAKY"], dl[0].Reason, dl[0].Fatal)
}

func TestRetryExhaustionDeadLettersWithReason(t *testing.T) {
	proc := func(ctx context.Context, tx pipeline.Tx, d event.Delivery) error {
		return errors.New("DOWNSTREAM_LOCKED") // plain error => retryable
	}
	p, _ := newPipeline(t, proc, pipeline.Config{MaxAttempts: 3, BackoffInitial: time.Millisecond})
	if _, err := p.Publish(context.Background(), "k", "STUCK", "t", nil); err != nil {
		t.Fatal(err)
	}
	waitIdle(t, p, 5*time.Second)

	dl := p.DeadLetters()
	if len(dl) != 1 || dl[0].Fatal != false || dl[0].Attempts != 3 {
		t.Fatalf("want one retry-exhausted record with 3 attempts, got %+v", dl)
	}
	if !contains(dl[0].Reason, "DOWNSTREAM_LOCKED") {
		t.Fatalf("reason not preserved: %q", dl[0].Reason)
	}
}

func TestPerKeyOrderAndCrossKeyConcurrency(t *testing.T) {
	const keys = 6
	const perKey = 20

	var maxConcurrency int32
	var cur int32
	var mu sync.Mutex
	order := map[string][]int{}

	proc := func(ctx context.Context, tx pipeline.Tx, d event.Delivery) error {
		n := atomic.AddInt32(&cur, 1)
		for {
			old := atomic.LoadInt32(&maxConcurrency)
			if n <= old || atomic.CompareAndSwapInt32(&maxConcurrency, old, n) {
				break
			}
		}
		// A small delay makes real parallelism observable.
		time.Sleep(2 * time.Millisecond)
		mu.Lock()
		order[d.Event.Key] = append(order[d.Event.Key], int(d.Event.Seq))
		mu.Unlock()
		atomic.AddInt32(&cur, -1)
		return nil
	}
	p, _ := newPipeline(t, proc, pipeline.Config{Concurrency: keys})

	ctx := context.Background()
	var wg sync.WaitGroup
	for k := 0; k < keys; k++ {
		wg.Add(1)
		go func(k int) {
			defer wg.Done()
			key := "key-" + itoa(k)
			for i := 1; i <= perKey; i++ {
				if _, err := p.Publish(ctx, key, "evt-"+key+"-"+itoa(i), "t", nil); err != nil {
					t.Error(err)
				}
			}
		}(k)
	}
	wg.Wait()
	waitIdle(t, p, 10*time.Second)

	mu.Lock()
	defer mu.Unlock()
	for k := 0; k < keys; k++ {
		key := "key-" + itoa(k)
		got := order[key]
		if len(got) != perKey {
			t.Fatalf("%s processed %d events, want %d", key, len(got), perKey)
		}
		for i, seq := range got {
			if seq != i+1 {
				t.Fatalf("%s processing order wrong: %v", key, got)
			}
		}
	}
	if atomic.LoadInt32(&maxConcurrency) < 2 {
		t.Fatalf("different keys never ran concurrently (max=%d)", maxConcurrency)
	}
	t.Logf("判定依据: 每个 key 顺序严格 1..%d; 观测到最大并发=%d (不同 key 并行)", perKey, maxConcurrency)
}

func TestRestartResumesFromCheckpointAndMatchesSequential(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	cfg := pipeline.Config{BackoffInitial: time.Millisecond}

	openPipeline := func() (*pipeline.Pipeline, *store.Store) {
		st, err := store.Open(context.Background(), dir)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		c := cfg
		c.Logger = testLogger(t)
		p := pipeline.NewPipeline(st, countingProcessor(t), c)
		if err := p.Start(context.Background()); err != nil {
			t.Fatalf("start: %v", err)
		}
		return p, st
	}

	p, st := openPipeline()
	ctx := context.Background()
	for i := 1; i <= 4; i++ {
		if _, err := p.Publish(ctx, "k", "e"+itoa(i), "inc", nil); err != nil {
			t.Fatal(err)
		}
	}
	// Process only up through e2 deterministically by polling the checkpoint.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if p.View().Checkpoints["k"] >= 2 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	// Hard stop: close runtime and store, simulating a process restart.
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	ckptBefore := storeCheckpoint(t, dir, "k")
	if ckptBefore < 2 {
		t.Fatalf("expected >=2 acked before restart, got %d", ckptBefore)
	}

	// Restart and publish more events; final state must equal a sequential
	// single run over e1..e6.
	p2, st2 := openPipeline()
	for i := 5; i <= 6; i++ {
		if _, err := p2.Publish(ctx, "k", "e"+itoa(i), "inc", nil); err != nil {
			t.Fatal(err)
		}
	}
	waitIdle(t, p2, 5*time.Second)
	if got := p2.View().State["counter"]; got != "6" {
		t.Fatalf("after restart counter = %q, want 6 (no lost or double effects)", got)
	}
	t.Logf("判定依据: 重启前 checkpoint=%d, 重启后 counter=%s (等价于顺序执行 e1..e6)",
		ckptBefore, p2.View().State["counter"])
	_ = p2.Close()
	_ = st2.Close()
}

func TestSeekRejectsBackwardAndUnrecoverable(t *testing.T) {
	proc := func(ctx context.Context, tx pipeline.Tx, d event.Delivery) error { return nil }
	p, _ := newPipeline(t, proc, pipeline.Config{})
	ctx := context.Background()
	for i := 1; i <= 3; i++ {
		if _, err := p.Publish(ctx, "k", "s"+itoa(i), "t", nil); err != nil {
			t.Fatal(err)
		}
	}
	waitIdle(t, p, 5*time.Second)

	// Everything acked: any seek is now "backward".
	if _, err := p.Seek(ctx, "k", 1); !errors.Is(err, pipeline.ErrSeekBackwards) {
		t.Fatalf("backward seek: want ErrSeekBackwards, got %v", err)
	}
	// Beyond the known log on a fresh key: unrecoverable position.
	if _, err := p.Seek(ctx, "brand-new", 9); !errors.Is(err, pipeline.ErrSeekOutOfRange) {
		t.Fatalf("out-of-range seek: want ErrSeekOutOfRange, got %v", err)
	}
	t.Log("判定依据: 回退到已确认位置 -> ErrSeekBackwards; 回退到不存在位置 -> ErrSeekOutOfRange")
}

func TestSeekForwardSkipsAndAudits(t *testing.T) {
	var mu sync.Mutex
	processed := []string{}
	proc := func(ctx context.Context, tx pipeline.Tx, d event.Delivery) error {
		mu.Lock()
		processed = append(processed, d.Event.ID)
		mu.Unlock()
		tx.Put("last", []byte(d.Event.ID))
		return nil
	}
	p, _ := newPipelineStopped(t, proc, pipeline.Config{})
	ctx := context.Background()
	for i := 1; i <= 3; i++ {
		if _, err := p.Publish(ctx, "k", "f"+itoa(i), "t", nil); err != nil {
			t.Fatal(err)
		}
	}

	// Before consumption starts, explicitly acknowledge forward to seq 2.
	// f1 and f2 must never reach the processor, and both IDs are returned for
	// the audit trail; seeking backward to 2 again is rejected.
	res, err := p.Seek(ctx, "k", 2)
	if err != nil {
		t.Fatalf("forward seek: %v", err)
	}
	if len(res.SkippedIDs) != 2 || res.SkippedIDs[0] != "f1" || res.SkippedIDs[1] != "f2" {
		t.Fatalf("skipped audit wrong: %+v", res)
	}
	if _, err := p.Seek(ctx, "k", 2); !errors.Is(err, pipeline.ErrSeekBackwards) {
		t.Fatalf("re-seek to same acked position: want ErrSeekBackwards, got %v", err)
	}

	if err := p.Start(ctx); err != nil {
		t.Fatal(err)
	}
	waitIdle(t, p, 5*time.Second)

	mu.Lock()
	defer mu.Unlock()
	if len(processed) != 1 || processed[0] != "f3" {
		t.Fatalf("processor saw %v, want only f3 after forward skip", processed)
	}
	if got := p.View().State["last"]; got != "f3" {
		t.Fatalf("last = %q, want f3", got)
	}
	t.Logf("判定依据: seek(0->2) 审计 skipped=%v, 消费器仅处理 %v, checkpoint=k:3", res.SkippedIDs, processed)
}

// ---- helpers ----------------------------------------------------------------

func countingProcessor(t *testing.T) pipeline.Processor {
	t.Helper()
	return func(ctx context.Context, tx pipeline.Tx, d event.Delivery) error {
		raw, ok := tx.Get("counter")
		n := 0
		if ok {
			for _, c := range string(raw) {
				n = n*10 + int(c-'0')
			}
		}
		n++
		t.Logf("event_id=%s attempt=%d progress=%d decision=commit counter=%d",
			d.Event.ID, d.Attempt, d.Event.Seq-1, n)
		tx.Put("counter", []byte(itoa(n)))
		return nil
	}
}

func storeCheckpoint(t *testing.T, dir, key string) uint64 {
	t.Helper()
	st, err := store.Open(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	return st.View().Checkpoints[key]
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}

// TestCrashMidStreamReplaysOnlyUnacked simulates a crash while events 3..5
// are still unacknowledged: after reopening the same directory their
// redelivery plus no double effect on 1..2 must match a sequential run.
func TestCrashMidStreamReplaysOnlyUnacked(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")

	publishOnly := func() {
		st, err := store.Open(context.Background(), dir)
		if err != nil {
			t.Fatal(err)
		}
		ctx := context.Background()
		for i := 1; i <= 5; i++ {
			if _, _, err := st.Publish(ctx, "k", "c"+itoa(i), "inc", nil); err != nil {
				t.Fatal(err)
			}
		}
		if err := st.Close(); err != nil {
			t.Fatal(err)
		}
	}
	publishOnly()

	// First consumer run: close as soon as checkpoint reaches 2.
	{
		st, err := store.Open(context.Background(), dir)
		if err != nil {
			t.Fatal(err)
		}
		p := pipeline.NewPipeline(st, countingProcessor(t),
			pipeline.Config{Logger: testLogger(t)})
		if err := p.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			if st.View().Checkpoints["k"] >= 2 {
				break
			}
			time.Sleep(time.Millisecond)
		}
		// Hard shutdown: emulate crash with in-flight/unacked events.
		_ = p.Close()
		_ = st.Close()
	}

	// Second consumer run over the same durable directory.
	st, err := store.Open(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	p := pipeline.NewPipeline(st, countingProcessor(t),
		pipeline.Config{Logger: testLogger(t)})
	if err := p.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitIdle(t, p, 5*time.Second)
	_ = p.Close()

	if got := st.View().Checkpoints["k"]; got != 5 {
		t.Fatalf("checkpoint=%d, want 5", got)
	}
	if got := st.View().State["counter"]; got != "5" {
		t.Fatalf("counter after crash recovery=%q, want 5 (acked 1..2 not replayed, 3..5 redelivered)", got)
	}
	t.Logf("判定依据: 崩溃点 checkpoint=2, 恢复后 checkpoint=5 counter=5 (等价顺序执行 c1..c5)")
}
