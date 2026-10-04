// Command demo 在本地数据目录上跑通一条异步事件投递与消费链路。
//
// 它演示：
//   - 正常事件：恰好生效一次；
//   - 重复投递：相同事件 ID 多次投递，业务效果只生效一次；
//   - 瞬态失败：注入前几次失败，重试后成功（效果一次）；
//   - 永久失败 / 非法负载：不重试进入死信，保留可区分的失败原因；
//   - 完成判定：WaitUntilIdle 等到“调用时已发布事件全部终态”才返回，
//     Progress 给出成功/死信/处理中/重试/排队/未派发的可解释进度。
//
// 运行（不需要任何外部中间件或网络）：
//
//	go run ./cmd/demo -dir ./.demo-data -clean     # 首轮：清空目录
//	go run ./cmd/demo -dir ./.demo-data           # 复用同一目录，追加新一轮事件
//	go run ./cmd/demo -dir ./.demo-data           # 再次复用：历史+新批次都处理干净
//	go run ./cmd/demo -dir ./.demo-data -replay   # 停机后全量重放，结果一致
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/highcumontoa/async-event-pipeline-go/pipeline"
)

func main() {
	dir := flag.String("dir", "./.demo-data", "本地数据目录（events.log + state.snapshot.json）")
	clean := flag.Bool("clean", false, "启动前清空数据目录（首次运行演示）")
	replay := flag.Bool("replay", false, "停机后执行一次显式 Replay(0) 全量重放")
	flag.Parse()

	if *clean {
		if err := os.RemoveAll(*dir); err != nil {
			fatal("clean dir: %v", err)
		}
	}
	if err := os.MkdirAll(*dir, 0o755); err != nil {
		fatal("mkdir: %v", err)
	}

	ctx := context.Background()
	cfg := pipeline.DefaultConfig(*dir)
	cfg.RetryBackoff = 50 * time.Millisecond
	cfg.MaxRetryBackoff = 500 * time.Millisecond
	cfg.Logger = pipeline.NewLogger(os.Stdout)

	// 第一轮打开：在打开前无法知道日志末尾；先打开、Start，再用 NextOffset
	// 作为本轮事件 ID 的基数，保证跨轮次复用目录时 ID 不冲突。
	p, h, err := openDemo(cfg)
	if err != nil {
		fatal("open: %v", err)
	}
	if err := p.Start(ctx); err != nil {
		fatal("start: %v", err)
	}

	priorEnd := p.Stats().NextOffset
	priorCommitted := p.Snapshot().CommittedOffset
	fmt.Printf("\n--- resume view: reused dir=%s, existing events=%d, committed=%d ---\n",
		*dir, priorEnd, priorCommitted)
	if priorEnd > 0 {
		printProgress(p, "progress recovered from previous run(s)")
	}

	// 本轮事件（ID 以历史日志末尾为基数，保证多轮复用全局唯一）。
	flakyID := fmt.Sprintf("flaky-%d", priorEnd)
	permID := fmt.Sprintf("perm-%d", priorEnd)
	h.FailNTimes(flakyID, 2).FailPermanent(permID, "demo: business rejects this event")
	batch := []pipeline.Event{
		{ID: fmt.Sprintf("ok-%d", priorEnd), Key: "tenant-a", Type: "inc", Payload: []byte("orders")},
		{ID: fmt.Sprintf("ok-%d", priorEnd), Key: "tenant-a", Type: "inc", Payload: []byte("orders")}, // 重复投递
		{ID: fmt.Sprintf("add-%d", priorEnd), Key: "tenant-b", Type: "add", Payload: []byte("orders=10")},
		{ID: flakyID, Key: "tenant-a", Type: "inc", Payload: []byte("retries")},                         // 失败重试
		{ID: permID, Key: "tenant-b", Type: "inc", Payload: []byte("rejected")},                         // 永久死信
		{ID: fmt.Sprintf("bad-%d", priorEnd), Key: "tenant-a", Type: "add", Payload: []byte("garbage")}, // 非法负载
		{ID: fmt.Sprintf("tail-%d", priorEnd), Key: "tenant-a", Type: "inc", Payload: []byte("orders")},
	}

	for _, ev := range batch {
		off, err := p.Publish(ev)
		if err != nil {
			fatal("publish %s: %v", ev.ID, err)
		}
		fmt.Printf(">>> published event=%s offset=%d key=%s\n", ev.ID, off, ev.Key)
	}

	// 关键：等待“调用时已发布事件全部到达终态”，而不是按本次发布条数等待。
	wait, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := p.WaitUntilIdle(wait); err != nil {
		printProgress(p, "WaitUntilIdle returned early (unexpected for the demo)")
		fatal("WaitUntilIdle: %v", err)
	}
	printProgress(p, "progress immediately after WaitUntilIdle (must already be complete)")

	if err := p.Drain(ctx); err != nil {
		fatal("drain: %v", err)
	}
	printState(p, "final durable state")

	if *replay {
		if err := p.Replay(ctx, 0); err != nil {
			fatal("replay: %v", err)
		}
		printState(p, "state after explicit Replay(0) (should equal final state)")
	}
	if err := p.Close(); err != nil {
		fatal("close: %v", err)
	}
	fmt.Printf("\nartifacts persisted under %s:\n  %s\n  %s\n",
		*dir,
		filepath.Join(*dir, "events.log"),
		filepath.Join(*dir, "state.snapshot.json"))
	fmt.Println("rerun without -clean to append another batch onto the same directory")
}

func openDemo(cfg pipeline.Config) (*pipeline.Pipeline, *pipeline.CountHandler, error) {
	h := pipeline.NewCountHandler()
	p, err := pipeline.Open(cfg, h)
	return p, h, err
}

func printProgress(p *pipeline.Pipeline, title string) {
	r := p.Progress()
	fmt.Printf("\n=== %s ===\n", title)
	fmt.Printf("log end=%d committed=%d | processed=%d dead=%d processing=%d retrying=%d queued=%d not-dispatched=%d\n",
		r.LogEnd, r.CommittedOffset, r.Processed, r.Dead,
		r.Processing, r.WaitingRetry, r.Queued, r.NotDispatched)
	fmt.Printf("complete=%v\n  basis: %s\n", r.Complete, r.Basis)
	if len(r.Gaps) > 0 {
		fmt.Printf("unfinished offsets (up to %d):\n", len(r.Gaps))
		for _, g := range r.Gaps {
			extra := ""
			if g.Phase == "retrying" {
				extra = fmt.Sprintf(" attempts=%d wait_until=%s reason=%q", g.Attempts,
					g.WaitUntil.Format("15:04:05.000"), g.Reason)
			}
			fmt.Printf("  - offset=%d event=%s key=%s phase=%s%s\n", g.Offset, g.EventID, g.Key, g.Phase, extra)
		}
	}
	if len(r.Keys) > 0 {
		fmt.Println("per-key remaining:")
		for _, k := range r.Keys {
			fmt.Printf("  - key=%-8s total=%2d processed=%2d dead=%2d remaining=%d (proc=%d retry=%d queued=%d unread=%d)\n",
				k.Key, k.Total, k.Processed, k.Dead, k.Remaining,
				k.Processing, k.WaitingRetry, k.Queued, k.NotDispatched)
		}
	}
}

func printState(p *pipeline.Pipeline, title string) {
	snap := p.Snapshot()
	kv := snap.Business.(*pipeline.KVState)
	fmt.Printf("\n=== %s ===\n", title)
	fmt.Printf("committed offset: %d / next: %d\n", snap.CommittedOffset, p.Stats().NextOffset)
	fmt.Printf("business effects : %v\n", kv.Values)
	fmt.Printf("dead letters (%d):\n", len(snap.Dead))
	for _, d := range snap.Dead {
		fmt.Printf("  - event=%-10s offset=%d kind=%-9s attempts=%d reason=%q\n",
			d.EventID, d.Offset, d.Kind, d.Attempts, d.Reason)
	}
	// 停机后再做一次离线完成判定：停机状态若已全部终态，WaitUntilIdle 立即确认。
	idleCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := p.WaitUntilIdle(idleCtx); err != nil {
		if errors.Is(err, pipeline.ErrPipelineNotRunning) {
			fmt.Printf("offline idle check: NOT complete while stopped: %v\n", err)
		} else {
			fmt.Printf("offline idle check error: %v\n", err)
		}
	} else {
		fmt.Println("offline idle check: complete=true (all published events terminal while stopped)")
	}
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "demo failed: "+format+"\n", args...)
	os.Exit(1)
}
