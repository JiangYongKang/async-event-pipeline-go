package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"
)

const hourBackoff = time.Hour

// waitInternal 阻塞等待水位到 want（包内测试用）。
func waitInternal(t *testing.T, p *Pipeline, want int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := p.WaitForOffset(ctx, want); err != nil {
		t.Fatalf("wait committed=%d: %v", want, err)
	}
}

// 场景 N4（包内）：硬崩溃于失败事件重试中途，随后在“未 Start”状态下打开：
//   - Progress 必须把未完成事件如实计入 Unread、指出缺口，不得假报完成；
//   - WaitUntilIdle 必须返回 ErrPipelineNotRunning（停机不可能自行产生进展）；
//   - Start 恢复后等待必须收敛到完整，事件恰好生效一次。
func TestStoppedIncompleteRejectsFalseIdle(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir)
	cfg.Logger = testLoggerInternal(t)
	cfg.RetryBackoff = hourBackoff
	cfg.MaxRetryBackoff = hourBackoff

	p1, err := Open(cfg, NewCountHandler().FailNTimes("x", 1_000_000))
	if err != nil {
		t.Fatal(err)
	}
	if err := p1.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := p1.Publish(Event{ID: "g", Key: "K", Type: "inc", Payload: []byte("K")}); err != nil {
		t.Fatal(err)
	}
	if _, err := p1.Publish(Event{ID: "x", Key: "K", Type: "inc", Payload: []byte("K")}); err != nil {
		t.Fatal(err)
	}
	// 等 g 到终态（水位 1），x 仍在长退避中。
	waitInternal(t, p1, 1)
	crashProcess(p1) // 先取消运行时再关文件句柄，模拟进程被立即杀死
	t.Logf("crashed mid-retry: committed=1 logEnd=2, x has no terminal record")

	// 重新打开但不 Start：停机且存在未完成事件。
	cfg2 := DefaultConfig(dir)
	cfg2.Logger = testLoggerInternal(t)
	p2, err := Open(cfg2, NewCountHandler())
	if err != nil {
		t.Fatal(err)
	}
	defer p2.Close()

	pr := p2.Progress()
	if pr.Complete || pr.CommittedOffset != 1 || pr.LogEndOffset != 2 {
		t.Fatalf("stopped incomplete progress wrong:\n%s", pr.String())
	}
	if pr.Unread != 1 || pr.Running {
		t.Fatalf("want 1 unread while stopped, got:\n%s", pr.String())
	}
	if len(pr.GapOffsets) != 1 || pr.GapOffsets[0] != 1 {
		t.Fatalf("gaps=%v want [1]", pr.GapOffsets)
	}
	if kp := pr.Keys[0]; kp.Key != "K" || kp.Published != 2 || kp.Terminal != 1 || kp.Remaining != 1 {
		t.Fatalf("per-key progress wrong: %+v", kp)
	}
	if err := p2.WaitUntilIdle(context.Background()); !errors.Is(err, ErrPipelineNotRunning) {
		t.Fatalf("stopped incomplete wait err=%v want ErrPipelineNotRunning", err)
	}
	t.Logf("ASSERT stopped-incomplete rejected:\n%s", pr.String())

	// Start 恢复后未完成事件（不再注入失败）正常处理，等待收敛。
	if err := p2.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := p2.WaitUntilIdle(context.Background()); err != nil {
		t.Fatalf("wait idle after recovery: %v\n%s", err, p2.Progress().String())
	}
	if err := p2.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	values := p2.Snapshot().Business.(*KVState).Values
	if values["K"] != 2 {
		t.Fatalf("K=%d want 2 after recovery (g once + x once)", values["K"])
	}
	if len(p2.Snapshot().Dead) != 0 {
		t.Fatalf("no dead expected, got %+v", p2.Snapshot().Dead)
	}
	t.Logf("ASSERT recovered to idle: %s values=%v", p2.Progress().Basis, values)
}
