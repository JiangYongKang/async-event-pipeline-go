package pipeline

import (
	"context"
	"testing"
	"time"
)

// 包内场景：第一轮在重试中途“硬崩溃”（不等到终态、进程立即退出），
// 第二轮复用同一目录、在历史之上追加新批次；WaitUntilIdle 必须等到
// 历史事件与新追加事件全部终态，恢复路径上的新事件同样受可靠投递、
// 幂等与同键顺序保护，不漏算。
func TestIdleAfterCrashRestartThenAppend(t *testing.T) {
	dir := t.TempDir()

	cfg1 := DefaultConfig(dir)
	cfg1.Logger = testLoggerInternal(t)
	cfg1.MaxAttempts = 1_000_000
	cfg1.RetryBackoff = time.Hour
	p1, err := Open(cfg1, NewCountHandler().FailNTimes("e-flaky", 1_000_000))
	if err != nil {
		t.Fatal(err)
	}
	if err := p1.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	mustPub := func(p *Pipeline, ev Event) {
		t.Helper()
		if _, err := p.Publish(ev); err != nil {
			t.Fatal(err)
		}
	}
	mustPub(p1, Event{ID: "e1", Key: "k1", Type: "inc", Payload: []byte("c")})
	mustPub(p1, Event{ID: "e-flaky", Key: "k2", Type: "inc", Payload: []byte("c")})
	wait1 := func(p *Pipeline, want int64) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := p.WaitForOffset(ctx, want); err != nil {
			t.Fatal(err)
		}
	}
	wait1(p1, 1)
	crashProcess(p1) // 硬崩溃：e-flaky 停在重试中，无终态
	t.Logf("crashed at committed=%d while e-flaky retrying", p1.Snapshot().CommittedOffset)

	// 第二轮：复用目录，Handler 不再注入失败；历史 e-flaky 与新批次一起处理。
	cfg2 := DefaultConfig(dir)
	cfg2.Logger = testLoggerInternal(t)
	cfg2.RetryBackoff = 2 * time.Millisecond
	p2, err := Open(cfg2, NewCountHandler())
	if err != nil {
		t.Fatal(err)
	}
	if err := p2.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	mustPub(p2, Event{ID: "e2", Key: "k1", Type: "inc", Payload: []byte("c")})
	mustPub(p2, Event{ID: "e2", Key: "k1", Type: "inc", Payload: []byte("c")}) // 重复
	mustPub(p2, Event{ID: "e3", Key: "k1", Type: "inc", Payload: []byte("c")})

	// 必须等到历史 2 条 + 新 3 条全部终态。
	idleCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := p2.WaitUntilIdle(idleCtx); err != nil {
		r := p2.Progress()
		t.Fatalf("WaitUntilIdle after crash+append: %v basis=%s", err, r.Basis)
	}
	r := p2.Progress()
	if !r.Complete || r.LogEnd != 5 || r.CommittedOffset != 5 {
		t.Fatalf("after crash+append report wrong: %+v", r)
	}
	if err := p2.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	values := p2.Snapshot().Business.(*KVState).Values
	// k1: e1,e2,e3 各一次（重复不二次生效）=3；k2: 恢复后的 e-flaky=1。
	if values["c"] != 4 {
		t.Fatalf("c=%d want 4 (e1,e2,e3 + recovered e-flaky once): %v", values["c"], values)
	}
	if len(p2.Snapshot().Dead) != 0 {
		t.Fatalf("unexpected dead letters: %+v", p2.Snapshot().Dead)
	}
	t.Logf("ASSERT crash-restart-append: committed=%d values=%v basis=historical+new-batch all terminal",
		r.CommittedOffset, values)
}
