package pipeline

import (
	"context"
	"testing"
	"time"
)

// 场景 7（包内）：在失败事件重试中途“硬重启”（不等终态）。重新打开管道后，
// 事件必须被重新处理：恢复后成功生效且仅一次，不留下半完成状态。
func TestCrashMidRetryThenRecover(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir)
	cfg.Logger = testLoggerInternal(t)
	cfg.MaxAttempts = 1_000_000 // 第一次生命周期内永远重试，不会死信
	cfg.RetryBackoff = time.Hour

	p1, err := Open(cfg, NewCountHandler().FailNTimes("e-bad", 1_000_000))
	if err != nil {
		t.Fatal(err)
	}
	if err := p1.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := p1.Publish(Event{ID: "e-good", Key: "k1", Type: "inc", Payload: []byte("c")}); err != nil {
		t.Fatal(err)
	}
	if _, err := p1.Publish(Event{ID: "e-bad", Key: "k2", Type: "inc", Payload: []byte("c")}); err != nil {
		t.Fatal(err)
	}
	wait := func(p *Pipeline, want int64) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := p.WaitForOffset(ctx, want); err != nil {
			t.Fatal(err)
		}
	}
	wait(p1, 1)
	before := p1.Snapshot().CommittedOffset
	crashProcess(p1) // 模拟进程立即退出
	t.Logf("ASSERT crash: committed before=%d, e-bad left mid-retry without terminal state", before)

	// 第二次生命周期：不再注入失败 -> e-bad 正常处理成功，水位补齐。
	cfg2 := DefaultConfig(dir)
	cfg2.Logger = testLoggerInternal(t)
	p2, err := Open(cfg2, NewCountHandler())
	if err != nil {
		t.Fatal(err)
	}
	defer p2.Close()
	if err := p2.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	wait(p2, 2)
	if err := p2.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	snap := p2.Snapshot()
	values := snap.Business.(*KVState).Values
	if values["c"] != 2 {
		t.Fatalf("after crash recovery values=%v want c:2 (e-good once, e-bad once after fix)", values)
	}
	if len(snap.Dead) != 0 {
		t.Fatalf("no dead letters expected after successful recovery, got %+v", snap.Dead)
	}
	t.Logf("ASSERT crash recovery: committed=%d values=%v dead=0 basis=unfinished event reprocessed from log",
		snap.CommittedOffset, values)
}

// testLoggerInternal 桥接 testing.T 到 Logger（包内测试用）。
type tLogger struct{ t *testing.T }

func (l tLogger) Infof(format string, args ...any) {
	l.t.Helper()
	l.t.Logf("LOG INFO  "+format, args...)
}
func (l tLogger) Warnf(format string, args ...any) {
	l.t.Helper()
	l.t.Logf("LOG WARN  "+format, args...)
}
func (l tLogger) Errorf(format string, args ...any) {
	l.t.Helper()
	l.t.Logf("LOG ERROR "+format, args...)
}

func testLoggerInternal(t *testing.T) Logger { return tLogger{t: t} }
