package pipeline_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/highcumontoa/async-event-pipeline-go/pipeline"
)

// testLog 把运行日志桥接到 testing.T，日志中包含事件标识、处理进度与判定依据。
type testLog struct{ t *testing.T }

func (l testLog) Infof(format string, args ...any)  { l.t.Logf("LOG INFO  "+format, args...) }
func (l testLog) Warnf(format string, args ...any)  { l.t.Logf("LOG WARN  "+format, args...) }
func (l testLog) Errorf(format string, args ...any) { l.t.Logf("LOG ERROR "+format, args...) }

func testConfig(t *testing.T, dir string) pipeline.Config {
	t.Helper()
	cfg := pipeline.DefaultConfig(dir)
	cfg.RetryBackoff = 2 * time.Millisecond
	cfg.MaxRetryBackoff = 20 * time.Millisecond
	cfg.PollInterval = 5 * time.Millisecond
	cfg.Logger = testLog{t: t}
	return cfg
}

func waitCommitted(t *testing.T, p *pipeline.Pipeline, want int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := p.WaitForOffset(ctx, want); err != nil {
		t.Fatalf("wait committed=%d failed: %v (stats=%+v)", want, err, p.Stats())
	}
}

func kvOf(t *testing.T, p *pipeline.Pipeline) map[string]int64 {
	t.Helper()
	kv, ok := p.Snapshot().Business.(*pipeline.KVState)
	if !ok {
		t.Fatalf("business state is not *KVState: %T", p.Snapshot().Business)
	}
	return kv.Values
}

// 场景 1：重复投递。同一事件 ID 被多次投递时业务效果只生效一次；
// 同键顺序保证后投递的不同事件效果照常生效。
func TestDuplicateDelivery(t *testing.T) {
	dir := t.TempDir()
	p, err := pipeline.Open(testConfig(t, dir), pipeline.NewCountHandler())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	// e1 重复投递 3 次（相同 ID、相同 Key/负载）；e2 是同键的后续事件。
	_, _ = p.Publish(pipeline.Event{ID: "e1", Key: "k1", Type: "inc", Payload: []byte("counter")})
	_, _ = p.Publish(pipeline.Event{ID: "e1", Key: "k1", Type: "inc", Payload: []byte("counter")})
	_, _ = p.Publish(pipeline.Event{ID: "e1", Key: "k1", Type: "inc", Payload: []byte("counter")})
	_, _ = p.Publish(pipeline.Event{ID: "e2", Key: "k1", Type: "inc", Payload: []byte("counter")})
	waitCommitted(t, p, 4)

	if err := p.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	snap := p.Snapshot()
	if snap.CommittedOffset != 4 {
		t.Fatalf("committed=%d want 4", snap.CommittedOffset)
	}
	values := kvOf(t, p)
	if values["counter"] != 2 {
		t.Fatalf("counter=%d want 2 (e1 once + e2 once): duplicate event took effect more than once",
			values["counter"])
	}
	if rec, ok := p.StateStore().Lookup("e1"); !ok || rec.Status != pipeline.StatusProcessed {
		t.Fatalf("e1 record missing or wrong: %+v", rec)
	}
	t.Logf("ASSERT duplicate: counter=%d (want 2), committed=%d, basis=3x same id only first applied",
		values["counter"], snap.CommittedOffset)
}

// 场景 2：处理失败重试。
//   - 瞬态错误在前 N 次失败、重试成功：效果恰好一次，重试数可观察；
//   - 瞬态错误持续超过 MaxAttempts：进入死信，kind=transient，原因可区分；
//   - 永久错误：不重试直接死信，kind=permanent；
//   - 非法负载：不重试直接死信，kind=invalid。
func TestFailureRetryAndDeadLetter(t *testing.T) {
	dir := t.TempDir()
	h := pipeline.NewCountHandler().
		FailNTimes("e-recover", 3).                       // 前 3 次瞬态失败，第 4 次成功
		FailNTimes("e-boom", 1000).                       // 在 MaxAttempts 内始终失败 -> transient 死信
		FailPermanent("e-perm", "business rule violated") // 永久失败
	cfg := testConfig(t, dir)
	cfg.MaxAttempts = 4
	p, err := pipeline.Open(cfg, h)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	_, _ = p.Publish(pipeline.Event{ID: "e-recover", Key: "k1", Type: "inc", Payload: []byte("ok")})
	_, _ = p.Publish(pipeline.Event{ID: "e-perm", Key: "k1", Type: "inc", Payload: []byte("p")})
	_, _ = p.Publish(pipeline.Event{ID: "e-boom", Key: "k2", Type: "inc", Payload: []byte("b")})
	_, _ = p.Publish(pipeline.Event{ID: "e-bad", Key: "k3", Type: "add", Payload: []byte("not-a-key-value")})
	waitCommitted(t, p, 4)
	if err := p.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}

	values := kvOf(t, p)
	if values["ok"] != 1 {
		t.Fatalf("ok=%d want 1 (recovered event applied exactly once)", values["ok"])
	}
	if _, exists := values["b"]; exists {
		t.Fatalf("failed transient event must not leave partial business effect")
	}

	dead := p.Snapshot().Dead
	if len(dead) != 3 {
		t.Fatalf("dead letters=%d want 3: %+v", len(dead), dead)
	}
	byID := map[string]pipeline.DeadRecord{}
	for _, d := range dead {
		byID[d.EventID] = d
	}
	tr, ok := byID["e-boom"]
	if !ok || tr.Kind != pipeline.FailureTransient || tr.Attempts != 4 {
		t.Fatalf("e-boom dead record wrong: %+v", tr)
	}
	pm, ok := byID["e-perm"]
	if !ok || pm.Kind != pipeline.FailurePermanent || pm.Reason != "business rule violated" {
		t.Fatalf("e-perm dead record wrong: %+v", pm)
	}
	iv, ok := byID["e-bad"]
	if !ok || iv.Kind != pipeline.FailureInvalid {
		t.Fatalf("e-bad dead record wrong: %+v", iv)
	}
	t.Logf("ASSERT dead reasons distinguishable: transient(%s,attempts=%d) permanent(%s,%q) invalid(%s,%q); values=%v",
		tr.Kind, tr.Attempts, pm.Kind, pm.Reason, iv.Kind, iv.Reason, values)
}

// 场景 3：并发处理。大量不同业务键并行推进，同时混入同键顺序事件与
// 重复投递；最终每个键的计数必须严格等于其“不同事件 ID 数”。
func TestConcurrentProcessing(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, dir)
	cfg.Parallelism = 8
	p, err := pipeline.Open(cfg, pipeline.NewCountHandler())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	const keys = 20
	const perKey = 15
	var total int64
	var wg sync.WaitGroup
	pubErr := make(chan error, 1)
	for k := 0; k < keys; k++ {
		key := fmt.Sprintf("key-%02d", k)
		for i := 0; i < perKey; i++ {
			id := fmt.Sprintf("%s#%02d", key, i)
			wg.Add(1)
			total++
			go func(id, key string, dup bool) {
				defer wg.Done()
				if _, err := p.Publish(pipeline.Event{ID: id, Key: key, Type: "inc", Payload: []byte(key)}); err != nil {
					pubErr <- err
					return
				}
				if dup {
					// 并发重复投递：不同 goroutine 发布同一 ID。
					if _, err := p.Publish(pipeline.Event{ID: id, Key: key, Type: "inc", Payload: []byte(key)}); err != nil {
						pubErr <- err
					}
				}
			}(id, key, i%3 == 0)
		}
	}
	wg.Wait()
	close(pubErr)
	for err := range pubErr {
		t.Fatal(err)
	}

	// 唯一事件 total 条，重复副本 total/3 条。
	expectedOffset := total + total/3
	waitCommitted(t, p, expectedOffset)
	if err := p.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	values := kvOf(t, p)
	if len(values) != keys {
		t.Fatalf("got %d distinct keys want %d", len(values), keys)
	}
	for k := 0; k < keys; k++ {
		key := fmt.Sprintf("key-%02d", k)
		if values[key] != int64(perKey) {
			t.Fatalf("key %s count=%d want %d (ordering/idempotency broken)", key, values[key], perKey)
		}
	}
	t.Logf("ASSERT concurrency: %d unique + %d duplicate deliveries => each of %d keys=%d, committed=%d",
		total, total/3, keys, perKey, p.Snapshot().CommittedOffset)
}

// 场景 4：同键顺序。同一业务键上的事件严格按产生顺序生效。
// 使用 add 事件让乱序必然产生可观察的错误结果（+1/-1 序列的最终值与顺序无关，
// 因此改用断言处理记录的 offset 顺序与递增计数中间状态：通过自定义 Handler
// 记录每次生效的事件偏移序列）。
func TestPerKeyOrdering(t *testing.T) {
	dir := t.TempDir()
	var mu sync.Mutex
	var seenA []int64
	var seenB []int64
	h := pipeline.HandlerFunc(func(_ context.Context, ev pipeline.StoredEvent, bs pipeline.BusinessState) error {
		mu.Lock()
		defer mu.Unlock()
		switch ev.Event.Key {
		case "A":
			seenA = append(seenA, ev.Offset)
		case "B":
			seenB = append(seenB, ev.Offset)
		}
		kv := bs.(*pipeline.KVState)
		kv.Values[ev.Event.Key]++
		return nil
	})
	cfg := testConfig(t, dir)
	cfg.Parallelism = 2
	p, err := pipeline.Open(cfg, h)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	const n = 25
	for i := 0; i < n; i++ {
		key := "A"
		if i%2 == 0 {
			key = "B"
		}
		_, _ = p.Publish(pipeline.Event{
			ID:      fmt.Sprintf("ord-%02d", i),
			Key:     key,
			Type:    "inc",
			Payload: []byte(key),
		})
	}
	waitCommitted(t, p, n)
	if err := p.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	// CAS 版本过期时同一事件可能被重新施加（reapply），因此调用次数可 >= n；
	// 顺序保证体现为：每个键看到的“去重后偏移序列”严格递增且无缺失。
	ua := uniqueSorted(seenA)
	ub := uniqueSorted(seenB)
	if len(ua)+len(ub) != n {
		t.Fatalf("distinct processed=%d want %d: A=%v B=%v", len(ua)+len(ub), n, ua, ub)
	}
	if !sorted(ua) || !sorted(ub) {
		t.Fatalf("per-key order violated: A=%v B=%v", ua, ub)
	}
	values := kvOf(t, p)
	if values["A"] != int64(len(ua)) || values["B"] != int64(len(ub)) {
		t.Fatalf("business counts wrong: A=%d B=%d, values=%v", len(ua), len(ub), values)
	}
	t.Logf("ASSERT ordering: A offsets=%v B offsets=%v (strictly increasing, each applied once)", ua, ub)
}

func uniqueSorted(xs []int64) []int64 {
	seen := make(map[int64]struct{}, len(xs))
	out := make([]int64, 0, len(xs))
	for _, x := range xs {
		if _, ok := seen[x]; ok {
			continue
		}
		seen[x] = struct{}{}
		out = append(out, x)
	}
	return out
}

func sorted(xs []int64) bool {
	for i := 1; i < len(xs); i++ {
		if xs[i] <= xs[i-1] {
			return false
		}
	}
	return true
}

// 场景 5：重启恢复。处理一部分事件后关闭管道、重新打开并从持久化水位继续，
// 最终状态必须与一口气顺序执行一致；包括“崩溃于失败事件的重试之间”。
func TestRestartRecovery(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, dir)
	cfg.MaxAttempts = 100 // 第一次生命周期内故意不让它成功

	open := func() (*pipeline.Pipeline, *pipeline.CountHandler) {
		h := pipeline.NewCountHandler().FailNTimes("e-flaky", 1000)
		cc := testConfig(t, dir)
		cc.MaxAttempts = 3
		p, err := pipeline.Open(cc, h)
		if err != nil {
			t.Fatal(err)
		}
		return p, h
	}

	p, _ := open()
	if err := p.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, _ = p.Publish(pipeline.Event{ID: "e1", Key: "k1", Type: "inc", Payload: []byte("c")})
	_, _ = p.Publish(pipeline.Event{ID: "e-flaky", Key: "k2", Type: "inc", Payload: []byte("c")})
	waitCommitted(t, p, 1) // 只等 e1 完成；e-flaky 在重试中
	// 模拟重启：Drain 给短暂时间（失败事件会进入死信，因为 MaxAttempts=3）。
	dctx, dcancel := context.WithTimeout(context.Background(), time.Second)
	defer dcancel()
	if err := p.Drain(dctx); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	deadBefore := len(p.Snapshot().Dead)
	t.Logf("before restart: committed=%d dead=%d", p.Snapshot().CommittedOffset, deadBefore)

	// 重新打开：水位从快照恢复；继续投递后续事件，状态衔接一致。
	p2, err := pipeline.Open(testConfig(t, dir), pipeline.NewCountHandler())
	if err != nil {
		t.Fatal(err)
	}
	defer p2.Close()
	if err := p2.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, _ = p2.Publish(pipeline.Event{ID: "e2", Key: "k1", Type: "inc", Payload: []byte("c")})
	waitCommitted(t, p2, 3)
	if err := p2.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	values := kvOf(t, p2)
	if values["c"] != 2 {
		t.Fatalf("after restart c=%d want 2 (e1+e2); flaky must not partially apply", values["c"])
	}
	if len(p2.Snapshot().Dead) != 1 || p2.Snapshot().Dead[0].EventID != "e-flaky" {
		t.Fatalf("dead after restart mismatch: %+v", p2.Snapshot().Dead)
	}
	t.Logf("ASSERT restart: committed=%d values=%v dead=%d basis=watermark recovered from snapshot",
		p2.Snapshot().CommittedOffset, values, len(p2.Snapshot().Dead))
}

// 场景 6：显式重放与拒绝回退。
//   - Seek 到已提交位置之前 -> ErrRollbackRejected；
//   - Seek 到日志末尾之后 -> ErrOffsetBeyondEnd；
//   - Replay 到不可恢复位置（<0）-> ErrOffsetBeforeEarliest；
//   - 停机后 Replay(0) 全量重放，最终状态与顺序执行一致，重复事件不二次生效。
func TestReplayAndRejectRollback(t *testing.T) {
	dir := t.TempDir()
	p, err := pipeline.Open(testConfig(t, dir), pipeline.NewCountHandler())
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, _ = p.Publish(pipeline.Event{ID: "r1", Key: "k1", Type: "inc", Payload: []byte("x")})
	_, _ = p.Publish(pipeline.Event{ID: "r1", Key: "k1", Type: "inc", Payload: []byte("x")}) // 重复
	_, _ = p.Publish(pipeline.Event{ID: "r2", Key: "k2", Type: "inc", Payload: []byte("y")})
	waitCommitted(t, p, 3)

	rctx, rcancel := context.WithTimeout(context.Background(), time.Second)
	defer rcancel()
	if err := p.Seek(rctx, 0); !errors.Is(err, pipeline.ErrRollbackRejected) {
		t.Fatalf("seek backward err=%v want ErrRollbackRejected", err)
	}
	if err := p.Seek(rctx, 100); !errors.Is(err, pipeline.ErrOffsetBeyondEnd) {
		t.Fatalf("seek beyond end err=%v want ErrOffsetBeyondEnd", err)
	}
	if err := p.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := p.Replay(context.Background(), -1); !errors.Is(err, pipeline.ErrOffsetBeforeEarliest) {
		t.Fatalf("replay -1 err=%v want ErrOffsetBeforeEarliest", err)
	}
	if err := p.Replay(context.Background(), 1); !errors.Is(err, pipeline.ErrOffsetBeforeEarliest) {
		t.Fatalf("replay mid-stream err=%v want ErrOffsetBeforeEarliest", err)
	}
	// 运行中 Replay 必须被拒绝。
	_ = p.Start(context.Background())
	if err := p.Replay(context.Background(), 0); !errors.Is(err, pipeline.ErrPipelineRunning) {
		t.Fatalf("replay while running err=%v want ErrPipelineRunning", err)
	}
	if err := p.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}

	if err := p.Replay(context.Background(), 0); err != nil {
		t.Fatalf("explicit replay failed: %v", err)
	}
	snap := p.Snapshot()
	if snap.CommittedOffset != 3 {
		t.Fatalf("after replay committed=%d want 3", snap.CommittedOffset)
	}
	values := kvOf(t, p)
	if values["x"] != 1 || values["y"] != 1 {
		t.Fatalf("after replay values=%v want x:1 y:1 (idempotent reprocessing)", values)
	}
	if len(snap.Dead) != 0 {
		t.Fatalf("after replay dead=%+v want none", snap.Dead)
	}
	t.Logf("ASSERT replay: rebuilt committed=%d values=%v basis=full sequential replay with idempotency",
		snap.CommittedOffset, values)
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
}

// 场景 8：Handler panic 被当作瞬态错误重试，最终成功；该键队列不被卡死。
func TestHandlerPanicRecovered(t *testing.T) {
	dir := t.TempDir()
	var calls int32
	var mu sync.Mutex
	h := pipeline.HandlerFunc(func(_ context.Context, ev pipeline.StoredEvent, bs pipeline.BusinessState) error {
		mu.Lock()
		calls++
		n := calls
		mu.Unlock()
		if n == 1 {
			panic("boom")
		}
		bs.(*pipeline.KVState).Values["panic-key"]++
		return nil
	})
	p, err := pipeline.Open(testConfig(t, dir), h)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, _ = p.Publish(pipeline.Event{ID: "p1", Key: "k1", Type: "inc", Payload: []byte("panic-key")})
	waitCommitted(t, p, 1)
	if err := p.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if v := kvOf(t, p)["panic-key"]; v != 1 {
		t.Fatalf("panic-key=%d want 1 (panic retried, applied once)", v)
	}
	t.Logf("ASSERT panic: value=1 after 1 panic + 1 success, basis=panic-as-transient-retry")
}
