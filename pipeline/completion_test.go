package pipeline_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/highcumontoa/async-event-pipeline-go/pipeline"
)

// idleWait 用较宽松的超时等待真实空闲，失败时打印完整进度便于诊断。
func idleWait(t *testing.T, p *pipeline.Pipeline) pipeline.ProgressReport {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := p.WaitUntilIdle(ctx); err != nil {
		r := p.Progress()
		t.Fatalf("WaitUntilIdle: %v; report: %s gaps=%d keys=%d", err, r.Basis, len(r.Gaps), len(r.Keys))
	}
	r := p.Progress()
	if !r.Complete {
		t.Fatalf("after WaitUntilIdle progress not complete: %s", r.Basis)
	}
	t.Logf("idle confirmed: log_end=%d committed=%d processed=%d dead=%d basis=%s",
		r.LogEnd, r.CommittedOffset, r.Processed, r.Dead, r.Basis)
	return r
}

// 场景 A：连续多轮复用同一个数据目录，每轮恢复后继续追加事件。
// 新批次必须与历史事件享有同样的可靠投递/幂等/同键顺序保证，并且
// WaitUntilIdle 在每轮都等到“全部已发布事件终态”，不漏算历史与新批次。
func TestWaitUntilIdleAcrossRestartsAndAppends(t *testing.T) {
	dir := t.TempDir()
	const rounds = 3

	// 累计期望值：每轮发布不同事件，orders 键有正常+重复，retries 键有
	// 先失败后成功的事件；死信每轮 2 条（permanent + invalid）。
	var wantOrders int64
	var wantDead int
	for round := 0; round < rounds; round++ {
		h := pipeline.NewCountHandler()
		cfg := testConfig(t, dir)
		cfg.MaxAttempts = 5
		p, err := pipeline.Open(cfg, h)
		if err != nil {
			t.Fatal(err)
		}
		if err := p.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		prior := p.Stats().NextOffset
		t.Logf("round=%d reopened dir, prior events=%d committed=%d",
			round, prior, p.Snapshot().CommittedOffset)

		flaky := fmt.Sprintf("flaky-r%d", round)
		perm := fmt.Sprintf("perm-r%d", round)
		h.FailNTimes(flaky, 3).FailPermanent(perm, "permanent in round")
		batch := []pipeline.Event{
			{ID: fmt.Sprintf("ok-%d-0", round), Key: "k1", Type: "inc", Payload: []byte("orders")},
			{ID: fmt.Sprintf("ok-%d-0", round), Key: "k1", Type: "inc", Payload: []byte("orders")}, // 重复
			{ID: flaky, Key: "k1", Type: "inc", Payload: []byte("retries")},
			{ID: perm, Key: "k2", Type: "inc", Payload: []byte("x")},
			{ID: fmt.Sprintf("bad-%d", round), Key: "k2", Type: "add", Payload: []byte("garbage")},
			{ID: fmt.Sprintf("ok-%d-1", round), Key: "k1", Type: "inc", Payload: []byte("orders")},
		}
		for _, ev := range batch {
			if _, err := p.Publish(ev); err != nil {
				t.Fatal(err)
			}
		}
		wantOrders += 2 // 首条 ok（重复不二次生效）+ 末条 ok；flaky 计在 retries 键
		wantDead += 2

		// 关键断言：等到日志中“所有”已发布事件（历史 prior 条 + 本轮 6 条）终态。
		r := idleWait(t, p)
		if r.LogEnd != prior+int64(len(batch)) {
			t.Fatalf("round %d log_end=%d want %d", round, r.LogEnd, prior+int64(len(batch)))
		}
		if err := p.Drain(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := p.Close(); err != nil {
			t.Fatal(err)
		}
	}

	// 最后一轮结束后：重新以停机方式打开做一次离线完成确认。
	p, err := pipeline.Open(testConfig(t, dir), pipeline.NewCountHandler())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	idleCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := p.WaitUntilIdle(idleCtx); err != nil {
		t.Fatalf("offline WaitUntilIdle after all rounds: %v", err)
	}
	snap := p.Snapshot()
	values := snap.Business.(*pipeline.KVState).Values
	if values["orders"] != wantOrders {
		t.Fatalf("orders=%d want %d across %d rounds", values["orders"], wantOrders, rounds)
	}
	if len(snap.Dead) != wantDead {
		t.Fatalf("dead=%d want %d", len(snap.Dead), wantDead)
	}
	t.Logf("ASSERT %d rounds reuse: committed=%d values=%v dead=%d",
		rounds, snap.CommittedOffset, values, len(snap.Dead))
}

// 场景 B：等待期间事件正在重试——WaitUntilIdle 必须穿透退避等到真正终态；
// 等待过程中 Progress 必须如实展示 retrying/剩余/每个键差距，Complete=false；
// 事件恢复后立即转为 Complete=true，不存在“先报完成”。
func TestProgressReflectsUnfinishedWhileRetrying(t *testing.T) {
	dir := t.TempDir()
	h := pipeline.NewCountHandler().FailNTimes("slow", 1000) // 测试期间持续失败
	cfg := testConfig(t, dir)
	cfg.MaxAttempts = 1_000_000
	cfg.RetryBackoff = 30 * time.Millisecond
	p, err := pipeline.Open(cfg, h)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, _ = p.Publish(pipeline.Event{ID: "done", Key: "k1", Type: "inc", Payload: []byte("a")})
	_, _ = p.Publish(pipeline.Event{ID: "slow", Key: "k2", Type: "inc", Payload: []byte("b")})

	// 等到 slow 进入重试阶段（它的第一次失败可能尚未发生，轮询观察）。
	deadline := time.Now().Add(3 * time.Second)
	var mid pipeline.ProgressReport
	for time.Now().Before(deadline) {
		mid = p.Progress()
		if mid.WaitingRetry >= 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if mid.WaitingRetry != 1 {
		t.Fatalf("expected 1 retrying event, report=%s", mid.Basis)
	}
	if mid.Complete || mid.LogEnd != 2 || mid.CommittedOffset != 1 {
		t.Fatalf("mid-retry report wrong: complete=%v committed=%d logend=%d",
			mid.Complete, mid.CommittedOffset, mid.LogEnd)
	}
	if len(mid.Gaps) != 1 || mid.Gaps[0].Phase != "retrying" || mid.Gaps[0].EventID != "slow" {
		t.Fatalf("gaps should show slow retrying: %+v", mid.Gaps)
	}
	var k2 *pipeline.KeyProgress
	for i := range mid.Keys {
		if mid.Keys[i].Key == "k2" {
			k2 = &mid.Keys[i]
		}
	}
	if k2 == nil || k2.Remaining != 1 || k2.WaitingRetry != 1 {
		t.Fatalf("per-key k2 progress wrong: %+v", k2)
	}
	t.Logf("mid-flight report: %s; gap event=%s offset=%d key=%s attempts=%d",
		mid.Basis, mid.Gaps[0].EventID, mid.Gaps[0].Offset, mid.Gaps[0].Key, mid.Gaps[0].Attempts)

	// WaitUntilIdle 在 slow 仍重试期间不得返回；用短 ctx 证明它确实在等待。
	shortCtx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	if err := p.WaitUntilIdle(shortCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitUntilIdle returned while event retrying: err=%v", err)
	}

	// 解除故障注入：重建一个不再失败的 Handler 需要重启管道。
	// 此处不断言 Drain 结果（持续失败事件在退避被反复中断时可能在 ctx 内
	// 耗尽尝试进入死信、Drain 成功；也可能 ctx 超时）；停机语义由专门用例覆盖。
	dctx, dcancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	_ = p.Drain(dctx)
	dcancel()
}

// 场景 B2：重试恢复后 WaitUntilIdle 返回且进度立刻完整（与上面拆开，
// 避免依赖 Drain 在持续失败下的行为）。
func TestWaitUntilIdleReturnsAfterRetryRecovers(t *testing.T) {
	dir := t.TempDir()
	h := pipeline.NewCountHandler().FailNTimes("slow", 3)
	cfg := testConfig(t, dir)
	cfg.RetryBackoff = 10 * time.Millisecond
	p, err := pipeline.Open(cfg, h)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, _ = p.Publish(pipeline.Event{ID: "slow", Key: "k1", Type: "inc", Payload: []byte("v")})

	r := idleWait(t, p)
	if r.Processed != 1 || r.Dead != 0 || !r.Complete {
		t.Fatalf("unexpected report after recovery: %+v", r)
	}
	if err := p.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// 场景 C：多键高并发混跑 + 并发重复投递，多个 goroutine 并发调用
// WaitUntilIdle/Progress。任何一次 Complete=true 都必须与独立事实一致：
// 水位连续、每个已发布偏移都有终态、业务计数可对账——绝不允许“没处理完
// 却报告已完成”的假阳性。
func TestIdleNoFalsePositiveUnderConcurrency(t *testing.T) {
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

	const keys = 24
	const perKey = 8
	var total int64
	var wg sync.WaitGroup
	for k := 0; k < keys; k++ {
		key := fmt.Sprintf("key-%02d", k)
		for i := 0; i < perKey; i++ {
			id := fmt.Sprintf("%s#%02d", key, i)
			wg.Add(1)
			total++
			go func(id, key string, dup bool) {
				defer wg.Done()
				_, _ = p.Publish(pipeline.Event{ID: id, Key: key, Type: "inc", Payload: []byte(key)})
				if dup {
					_, _ = p.Publish(pipeline.Event{ID: id, Key: key, Type: "inc", Payload: []byte(key)})
				}
			}(id, key, i%2 == 0)
		}
	}

	// 并发“完成观察器”：只要看到 Complete=true，就用独立读取的
	// 水位 / 终态偏移集合 / 日志末尾交叉验证，抓假阳性。
	stop := make(chan struct{})
	var obsWG sync.WaitGroup
	var falsePositive int32
	var observations int32
	observe := func() {
		defer obsWG.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			r := p.Progress()
			atomic.AddInt32(&observations, 1)
			if r.Complete {
				end := p.EventLog().NextOffset()
				committed := p.Snapshot().CommittedOffset
				// 完成结论覆盖到的 target 必须 <= 当前日志末尾，且水位已到 target，
				// 并且报告自洽：各类未完成计数均为 0。
				if r.IdleTarget > end || committed < r.IdleTarget ||
					r.Processing+r.WaitingRetry+r.Queued+r.NotDispatched != 0 ||
					r.CommittedOffset != r.IdleTarget {
					t.Errorf("FALSE POSITIVE complete: report target=%d committed=%d logend=%d live_end=%d basis=%s",
						r.IdleTarget, r.CommittedOffset, r.LogEnd, end, r.Basis)
					atomic.AddInt32(&falsePositive, 1)
					return
				}
				// 每个 [0,target) 偏移都必须能查到终态。
				terms := p.StateStore().TerminalOffsets()
				for off := int64(0); off < r.IdleTarget; off++ {
					if _, ok := terms[off]; !ok {
						t.Errorf("FALSE POSITIVE: offset=%d lacks terminal record but complete=true", off)
						atomic.AddInt32(&falsePositive, 1)
						return
					}
				}
			}
			time.Sleep(time.Millisecond)
		}
	}
	for i := 0; i < 4; i++ {
		obsWG.Add(1)
		go observe()
	}

	// 并发等待者：每次 nil 返回时当时已发布事件必须全部终态。
	waiterDone := make(chan struct{})
	go func() {
		defer close(waiterDone)
		for i := 0; i < 20; i++ {
			target := p.EventLog().NextOffset()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			if err := p.WaitUntilIdle(ctx); err != nil {
				cancel()
				t.Errorf("concurrent WaitUntilIdle: %v", err)
				return
			}
			cancel()
			if p.Snapshot().CommittedOffset < target {
				t.Errorf("waiter returned but committed=%d < captured target=%d",
					p.Snapshot().CommittedOffset, target)
				return
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()

	wg.Wait()
	expected := total + total/2
	r := idleWait(t, p)
	<-waiterDone
	close(stop)
	obsWG.Wait()

	if r.LogEnd != expected {
		t.Fatalf("log_end=%d want %d", r.LogEnd, expected)
	}
	values := kvOf(t, p)
	if len(values) != keys {
		t.Fatalf("distinct keys=%d want %d", len(values), keys)
	}
	for k := 0; k < keys; k++ {
		key := fmt.Sprintf("key-%02d", k)
		if values[key] != int64(perKey) {
			t.Fatalf("key %s count=%d want %d", key, values[key], perKey)
		}
	}
	if err := p.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Logf("ASSERT concurrency no-false-positive: observations=%d fp=%d events=%d committed=%d",
		observations, falsePositive, expected, p.Snapshot().CommittedOffset)
}

// 场景 D：停机状态语义——
//   - 未 Start 时已发布但未消费：WaitUntilIdle 返回 ErrPipelineNotRunning，
//     错误里带差距，Progress 显示 stopped 空洞；
//   - 处理完并 Drain 后（未 Close）：WaitUntilIdle 离线立即确认完成。
func TestOfflineWaitUntilIdleSemantics(t *testing.T) {
	dir := t.TempDir()
	p, err := pipeline.Open(testConfig(t, dir), pipeline.NewCountHandler())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	// 不 Start，直接发布：事件已 fsync 但没有消费者。
	_, _ = p.Publish(pipeline.Event{ID: "x1", Key: "k1", Type: "inc", Payload: []byte("a")})
	r0 := p.Progress()
	if r0.Complete || r0.NotDispatched != 1 || len(r0.Gaps) != 1 || r0.Gaps[0].Phase != "stopped" {
		t.Fatalf("stopped-pending report wrong: %+v gaps=%+v", r0, r0.Gaps)
	}
	t.Logf("stopped pending basis=%s gap=%+v", r0.Basis, r0.Gaps[0])
	idleCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := p.WaitUntilIdle(idleCtx); !errors.Is(err, pipeline.ErrPipelineNotRunning) {
		t.Fatalf("want ErrPipelineNotRunning, got %v", err)
	}

	// 启动并处理完，Drain 后离线判定应立即完成。
	if err := p.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	idleWait(t, p)
	if err := p.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	idleCtx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	if err := p.WaitUntilIdle(idleCtx2); err != nil {
		t.Fatalf("offline idle after drain: %v", err)
	}
	t.Log("ASSERT offline: pending while stopped rejected with gaps, idle after drain confirmed")
}

// 场景 E：发布后立即 Drain（dispatcher 可能还没读到事件），停机也必须等
// 到“调用 Drain 时已发布”的全部事件终态，不允许漏掉已 fsync 未读入的事件。
func TestDrainCoversUnreadPublished(t *testing.T) {
	const iterations = 12
	for i := 0; i < iterations; i++ {
		dir := t.TempDir()
		cfg := testConfig(t, dir)
		cfg.PollInterval = 2 * time.Millisecond
		p, err := pipeline.Open(cfg, pipeline.NewCountHandler())
		if err != nil {
			t.Fatal(err)
		}
		if err := p.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		const n = 40
		for j := 0; j < n; j++ {
			_, _ = p.Publish(pipeline.Event{
				ID:   fmt.Sprintf("i%d-e%d", i, j),
				Key:  fmt.Sprintf("k-%d", j%4),
				Type: "inc", Payload: []byte("c"),
			})
		}
		// 不做任何等待，立即 Drain。
		dctx, dcancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := p.Drain(dctx); err != nil {
			dcancel()
			t.Fatalf("iter %d drain: %v", i, err)
		}
		dcancel()
		snap := p.Snapshot()
		if snap.CommittedOffset != n {
			t.Fatalf("iter %d committed=%d want %d (drain missed unread events)",
				i, snap.CommittedOffset, n)
		}
		if err := p.Close(); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("ASSERT drain covers unread: %d iterations x %d events all terminal at shutdown", iterations, 40)
}

// 场景 F：旧版数据目录兼容——旧快照不持久化重复投递偏移别名（仅水位掩盖）。
// 重新 Open 必须依据日志对账自愈，Progress/WaitUntilIdle 不得出现“水位到了
// 但有偏移查不到终态”的假完成/假未完成。
func TestLegacySnapshotReconciledOnOpen(t *testing.T) {
	dir := t.TempDir()
	p, err := pipeline.Open(testConfig(t, dir), pipeline.NewCountHandler())
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, _ = p.Publish(pipeline.Event{ID: "dup", Key: "k1", Type: "inc", Payload: []byte("c")})
	_, _ = p.Publish(pipeline.Event{ID: "dup", Key: "k1", Type: "inc", Payload: []byte("c")})
	_, _ = p.Publish(pipeline.Event{ID: "other", Key: "k1", Type: "inc", Payload: []byte("c")})
	idleWait(t, p)
	if err := p.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}

	// 改写快照为旧格式：删除 offset_aliases 字段（重复偏移 1 的别名丢失）。
	statePath := filepath.Join(dir, "state.snapshot.json")
	raw, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}
	var generic map[string]any
	if err := json.Unmarshal(raw, &generic); err != nil {
		t.Fatal(err)
	}
	delete(generic, "offset_aliases")
	legacy, err := json.MarshalIndent(generic, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(statePath, legacy, 0o644); err != nil {
		t.Fatal(err)
	}

	// 重新打开：对账应补齐别名；即使不启动消费者，离线判定也应为完成。
	p2, err := pipeline.Open(testConfig(t, dir), pipeline.NewCountHandler())
	if err != nil {
		t.Fatalf("reopen legacy snapshot: %v", err)
	}
	defer p2.Close()
	idleCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := p2.WaitUntilIdle(idleCtx); err != nil {
		t.Fatalf("legacy snapshot should reconcile to complete, got: %v", err)
	}
	r := p2.Progress()
	if !r.Complete || r.Processed != 3 {
		t.Fatalf("after reconcile complete=%v processed=%d basis=%s", r.Complete, r.Processed, r.Basis)
	}
	terms := p2.StateStore().TerminalOffsets()
	for off := int64(0); off < 3; off++ {
		rec, ok := terms[off]
		if !ok {
			t.Fatalf("offset %d missing terminal after reconcile", off)
		}
		if off == 1 && rec.EventID != "dup" {
			t.Fatalf("offset 1 alias wrong: %+v", rec)
		}
	}
	if v := p2.Snapshot().Business.(*pipeline.KVState).Values["c"]; v != 2 {
		t.Fatalf("business c=%d want 2 after reconcile", v)
	}
	t.Logf("ASSERT legacy snapshot healed: committed=%d processed=%d basis=%s",
		r.CommittedOffset, r.Processed, r.Basis)
}

// 场景 G：WaitUntilIdle 的边界语义——调用之后追加的事件不属于本次等待；
// 追加后再次调用可覆盖新批次，两批都不丢。
func TestWaitUntilIdleBoundaryAcrossAppends(t *testing.T) {
	dir := t.TempDir()
	h := pipeline.NewCountHandler().FailNTimes("a", 2)
	cfg := testConfig(t, dir)
	cfg.RetryBackoff = 40 * time.Millisecond
	p, err := pipeline.Open(cfg, h)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, _ = p.Publish(pipeline.Event{ID: "a", Key: "k1", Type: "inc", Payload: []byte("v")})

	firstDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		firstDone <- p.WaitUntilIdle(ctx) // 目标=1（仅 a）
	}()
	time.Sleep(30 * time.Millisecond) // 等 a 进入重试窗口
	_, _ = p.Publish(pipeline.Event{ID: "b", Key: "k1", Type: "inc", Payload: []byte("v")})
	if err := <-firstDone; err != nil {
		t.Fatalf("first WaitUntilIdle: %v", err)
	}
	t.Log("first WaitUntilIdle returned (target captured before b); calling again to cover b")
	idleWait(t, p)
	if err := p.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if v := kvOf(t, p)["v"]; v != 2 {
		t.Fatalf("v=%d want 2 (a and b both applied exactly once)", v)
	}
	t.Log("ASSERT wait boundary: batch appended after call needs a second wait; neither batch lost")
}
