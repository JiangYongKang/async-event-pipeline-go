package pipeline_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/highcumontoa/async-event-pipeline-go/pipeline"
)

// waitIdle 在时限内等待管道真正空闲，失败时附带进度说明，杜绝“以为等到了”。
func waitIdle(t *testing.T, p *pipeline.Pipeline) pipeline.Progress {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := p.WaitUntilIdle(ctx); err != nil {
		t.Fatalf("WaitUntilIdle failed: %v\n%s", err, p.Progress().String())
	}
	return p.Progress()
}

func assertIdle(t *testing.T, p *pipeline.Pipeline, wantEnd int64) pipeline.Progress {
	t.Helper()
	pr := p.Progress()
	if !pr.Complete {
		t.Fatalf("reported not idle although wait returned:\n%s", pr.String())
	}
	if pr.LogEndOffset != wantEnd || pr.CommittedOffset != wantEnd {
		t.Fatalf("progress=%s want committed=logEnd=%d", pr.String(), wantEnd)
	}
	if pr.Handling+pr.Retrying+pr.Waiting+pr.Unread != 0 {
		t.Fatalf("in-flight work remains after idle: %s", pr.String())
	}
	if st := p.Stats(); st.NextOffset != wantEnd || st.CommittedOffset != wantEnd {
		t.Fatalf("stats next=%d committed=%d want %d", st.NextOffset, st.CommittedOffset, wantEnd)
	}
	t.Logf("ASSERT idle: %s basis=%s", pr.String(), pr.Basis)
	return pr
}

// 场景 N1：WaitUntilIdle 必须等到“真正空闲”。
//   - 含瞬态重试事件（等待期间事件一度在 retrying 状态）；
//   - 第一批等待进行中再并发追加第二批；
//   - 返回后状态必须完整，且之后不再有任何事件被消费（数字稳定）。
func TestWaitUntilIdleCoversAppendedBatch(t *testing.T) {
	dir := t.TempDir()
	h := pipeline.NewCountHandler().FailNTimes("a-flaky", 3)
	p, err := pipeline.Open(testConfig(t, dir), h)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	// 第一批：含一个要重试 3 次的事件。
	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("a-%d", i)
		if i == 2 {
			id = "a-flaky"
		}
		_, _ = p.Publish(pipeline.Event{ID: id, Key: "A", Type: "inc", Payload: []byte("A")})
	}

	// 消费启动后、尚未空闲时，再并发追加第二批（另一键）。
	go func() {
		time.Sleep(5 * time.Millisecond)
		for i := 0; i < 5; i++ {
			_, _ = p.Publish(pipeline.Event{
				ID: fmt.Sprintf("b-%d", i), Key: "B", Type: "inc", Payload: []byte("B"),
			})
		}
		t.Logf("second batch of 5 appended mid-flight")
	}()

	pr := waitIdle(t, p)
	assertIdle(t, p, 10)
	values := kvOf(t, p)
	if values["A"] != 5 || values["B"] != 5 {
		t.Fatalf("values=%v want A:5 B:5 (flaky applied once)", values)
	}
	if pr.Retrying != 0 {
		t.Fatalf("retrying=%d want 0 after idle", pr.Retrying)
	}
	// 返回后再观察一段：不允许有“漏网”事件随后才处理导致数字变化。
	before := p.Snapshot().CommittedOffset
	time.Sleep(50 * time.Millisecond)
	if after := p.Snapshot().CommittedOffset; after != before {
		t.Fatalf("watermark moved after idle verdict: %d -> %d (false positive)", before, after)
	}
	if err := p.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// 场景 N2：进度说明必须如实反映未处理事件的各状态。
// 用长重试退避把事件卡在“处理/重试”态观测：
//   - 未开始处理：Unread > 0（dispatcher 尚未读到，或队列前序未完成）；
//   - 等待重试：Retrying > 0；
//   - 每键剩余数量与日志/水位一致；
//   - 未完成时 Complete 必须为 false。
func TestProgressExplainsOutstandingWork(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, dir)
	cfg.Parallelism = 1
	cfg.RetryBackoff = 2 * time.Second // 事件会稳定停在 retrying
	cfg.MaxRetryBackoff = 2 * time.Second
	cfg.MaxAttempts = 100
	h := pipeline.NewCountHandler().FailNTimes("stuck", 50)
	p, err := pipeline.Open(cfg, h)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	// 先发布一条要卡住重试的事件（键 A），随后追加 3 条 A 键事件（必须排队）
	// 和 2 条 B 键事件。
	_, _ = p.Publish(pipeline.Event{ID: "stuck", Key: "A", Type: "inc", Payload: []byte("A")})
	for i := 0; i < 3; i++ {
		_, _ = p.Publish(pipeline.Event{ID: fmt.Sprintf("a-%d", i), Key: "A", Type: "inc", Payload: []byte("A")})
	}
	_, _ = p.Publish(pipeline.Event{ID: "b-0", Key: "B", Type: "inc", Payload: []byte("B")})
	_, _ = p.Publish(pipeline.Event{ID: "b-1", Key: "B", Type: "inc", Payload: []byte("B")})

	// 等到 stuck 进入退避：水位仍为 0，Retrying 必须可见。
	deadline := time.Now().Add(5 * time.Second)
	var pr pipeline.Progress
	for time.Now().Before(deadline) {
		pr = p.Progress()
		if pr.Retrying == 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if pr.Retrying != 1 {
		t.Fatalf("expected stuck event in retrying state:\n%s", pr.String())
	}
	if pr.Complete {
		t.Fatalf("must not report complete with outstanding work:\n%s", pr.String())
	}
	if pr.CommittedOffset != 0 || pr.LogEndOffset != 6 {
		t.Fatalf("committed=%d logEnd=%d want 0/6:\n%s", pr.CommittedOffset, pr.LogEndOffset, pr.String())
	}
	if len(pr.GapOffsets) != 6 {
		t.Fatalf("gaps=%v want all 6 offsets", pr.GapOffsets)
	}
	byKey := map[string]pipeline.KeyProgress{}
	for _, k := range pr.Keys {
		byKey[k.Key] = k
	}
	ka, ok := byKey["A"]
	if !ok || ka.Published != 4 || ka.Terminal != 0 || ka.Remaining != 4 {
		t.Fatalf("key A progress wrong: %+v", ka)
	}
	kb, ok := byKey["B"]
	if !ok || kb.Published != 2 || kb.Remaining != 2 {
		t.Fatalf("key B progress wrong: %+v", kb)
	}
	outstanding := pr.Handling + pr.Retrying + pr.Waiting + pr.Unread
	if outstanding != 6 {
		t.Fatalf("outstanding=%d want 6 (handling=%d retrying=%d waiting=%d unread=%d)",
			outstanding, pr.Handling, pr.Retrying, pr.Waiting, pr.Unread)
	}
	t.Logf("ASSERT progress before completion:\n%s", pr.String())

	// B 键与 A 键不同键，可并行：在 Parallelism=1 下 B 也会在 stuck 退避结束后
	// 才被调度。这里直接 Drain（中断退避，stuck 继续重试直到成功）验证最终收敛。
	if err := p.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	pr = p.Progress()
	if !pr.Complete {
		t.Fatalf("after drain must be idle:\n%s", pr.String())
	}
	values := kvOf(t, p)
	if values["A"] != 4 || values["B"] != 2 {
		t.Fatalf("values=%v want A:4 B:2 (stuck recovers after drain-broken backoff)", values)
	}
	t.Logf("ASSERT progress after drain:\n%s", pr.String())
}

// 场景 N3：复用同一数据目录，在旧进度上继续追加事件——新事件必须与历史
// 事件享有同样的可靠投递/幂等/同键顺序；WaitUntilIdle 返回时新追加批次
// 全部处理到位。连续复用目录两轮。
func TestResumeThenAppendReliable(t *testing.T) {
	dir := t.TempDir()

	// 第 1 轮：空目录，写入历史事件（含重试事件），优雅停机。
	p1, err := pipeline.Open(testConfig(t, dir), pipeline.NewCountHandler().FailNTimes("h-flaky", 2))
	if err != nil {
		t.Fatal(err)
	}
	if err := p1.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	hist := []pipeline.Event{
		{ID: "h1", Key: "K1", Type: "inc", Payload: []byte("K1")},
		{ID: "h1", Key: "K1", Type: "inc", Payload: []byte("K1")}, // 历史重复
		{ID: "h-flaky", Key: "K2", Type: "inc", Payload: []byte("K2")},
		{ID: "h2", Key: "K1", Type: "inc", Payload: []byte("K1")},
	}
	for _, ev := range hist {
		_, _ = p1.Publish(ev)
	}
	waitIdle(t, p1)
	if err := p1.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if kvOf(t, p1)["K1"] != 2 {
		t.Fatalf("round1 K1=%d want 2", kvOf(t, p1)["K1"])
	}
	t.Logf("round1 closed: %s", p1.Progress().String())
	if err := p1.Close(); err != nil {
		t.Fatal(err)
	}

	// 第 2 轮：复用目录，从旧水位恢复，只追加新事件（含与历史同键的事件、
	// 同 ID 重复、以及一个需要重试的新事件）。
	p2, err := pipeline.Open(testConfig(t, dir), pipeline.NewCountHandler().FailNTimes("n-flaky", 3))
	if err != nil {
		t.Fatal(err)
	}
	if pr := p2.Progress(); !pr.Complete {
		t.Fatalf("freshly reopened pipeline must see history as complete:\n%s", pr.String())
	}
	if err := p2.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	newEvents := []pipeline.Event{
		{ID: "n1", Key: "K1", Type: "inc", Payload: []byte("K1")}, // 同键顺序衔接
		{ID: "n-flaky", Key: "K3", Type: "inc", Payload: []byte("K3")},
		{ID: "h1", Key: "K1", Type: "inc", Payload: []byte("K1")}, // 跨重启重复 ID
		{ID: "n2", Key: "K1", Type: "inc", Payload: []byte("K1")},
		{ID: "n3", Key: "K2", Type: "inc", Payload: []byte("K2")},
	}
	base := p2.Stats().NextOffset // 应为 4
	if base != 4 {
		t.Fatalf("resumed next offset=%d want 4", base)
	}
	for _, ev := range newEvents {
		_, _ = p2.Publish(ev)
	}
	pr := waitIdle(t, p2)
	assertIdle(t, p2, base+int64(len(newEvents)))
	if err := p2.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	values := kvOf(t, p2)
	// K1: h1(去重后1) + h2 + n1 + h1重复(0) + n2 = 4
	if values["K1"] != 4 {
		t.Fatalf("K1=%d want 4 (history 2 + new unique 2, duplicate no effect)", values["K1"])
	}
	if values["K2"] != 2 || values["K3"] != 1 { // h-flaky+n3 ; n-flaky
		t.Fatalf("K2=%d K3=%d want 2/1 (new batch fully processed)", values["K2"], values["K3"])
	}
	// 旧事件终态记录仍在，且重复 ID 未二次生效。
	if rec, ok := p2.StateStore().Lookup("h1"); !ok || rec.Status != pipeline.StatusProcessed {
		t.Fatalf("history idempotent record lost: %+v", rec)
	}
	if len(p2.Snapshot().Dead) != 0 {
		t.Fatalf("no dead letters expected, got %+v", p2.Snapshot().Dead)
	}
	t.Logf("ASSERT resume+append: %s values=%v", pr.Basis, values)
	if err := p2.Close(); err != nil {
		t.Fatal(err)
	}

	// 第 3 轮：再次复用同一目录，什么都不追加——停机态 Progress 仍应是完整，
	// WaitUntilIdle 在停机且已完整时立即返回 nil。
	p3, err := pipeline.Open(testConfig(t, dir), pipeline.NewCountHandler())
	if err != nil {
		t.Fatal(err)
	}
	defer p3.Close()
	if err := p3.WaitUntilIdle(context.Background()); err != nil {
		t.Fatalf("idle wait while stopped-and-complete: %v", err)
	}
	if !p3.Progress().Complete {
		t.Fatalf("third reopen must report complete:\n%s", p3.Progress().String())
	}
}

// 场景 N5：连续多轮、多键、高并发混跑，绝不允许“没处理完却报告完成”。
// 每轮：并发发布大量事件（含重复）-> WaitUntilIdle -> 断言计数精确、
// 进度恒等式成立；再用同一目录进入下一轮。中途穿插在等待时追加新批次。
func TestIdleNoFalsePositiveUnderMixedLoad(t *testing.T) {
	dir := t.TempDir()
	p, err := pipeline.Open(testConfig(t, dir), pipeline.NewCountHandler())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	var expected map[string]int64 // 每键不同事件 ID 数（= 最终计数）
	var totalOffsets int64

	const rounds = 3
	for r := 0; r < rounds; r++ {
		const keys = 12
		const perKey = 20
		var wg sync.WaitGroup
		roundExpected := map[string]int64{}
		var roundOffsets int64
		var pubMu sync.Mutex
		for k := 0; k < keys; k++ {
			key := fmt.Sprintf("key-%02d", k)
			for i := 0; i < perKey; i++ {
				id := fmt.Sprintf("r%d-%s#%02d", r, key, i)
				pubMu.Lock()
				roundExpected[key]++
				roundOffsets++
				pubMu.Unlock()
				wg.Add(1)
				go func(id, key string, dup bool) {
					defer wg.Done()
					_, _ = p.Publish(pipeline.Event{ID: id, Key: key, Type: "inc", Payload: []byte(key)})
					if dup {
						_, _ = p.Publish(pipeline.Event{ID: id, Key: key, Type: "inc", Payload: []byte(key)})
						pubMu.Lock()
						roundOffsets++
						pubMu.Unlock()
					}
				}(id, key, i%4 == 0)
			}
		}
		// 在发布尚未结束时并发等待：不得提前返回（返回点必须覆盖全部已发布）。
		waitDone := make(chan error, 1)
		go func() {
			waitDone <- p.WaitUntilIdle(context.Background())
		}()
		wg.Wait()
		select {
		case err := <-waitDone:
			if err != nil {
				t.Fatalf("round %d idle wait: %v", r, err)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("round %d idle wait timeout:\n%s", r, p.Progress().String())
		}
		if expected == nil {
			expected = map[string]int64{}
		}
		for k, v := range roundExpected {
			expected[k] += v
		}
		totalOffsets += roundOffsets
		pr := assertIdle(t, p, totalOffsets)
		// 恒等式校验。
		if sum := pr.Succeeded + pr.Dead + pr.Handling + pr.Retrying + pr.Waiting + pr.Unread; sum != totalOffsets {
			t.Fatalf("round %d identity broken: sum=%d end=%d", r, sum, totalOffsets)
		}
		values := kvOf(t, p)
		for k, want := range expected {
			if values[k] != want {
				t.Fatalf("round %d key %s=%d want %d (false completion?)", r, k, values[k], want)
			}
		}
		t.Logf("ASSERT round %d idle with %d offsets, %d keys, values consistent", r, roundOffsets, len(expected))
	}
	if err := p.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
}
