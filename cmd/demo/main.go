// Command demo 在本地数据目录上跑通一条异步事件投递与消费链路。
//
// 它演示四类事件的最终状态：
//   - 正常事件：恰好生效一次；
//   - 重复投递：相同事件 ID 多次投递，业务效果只生效一次；
//   - 瞬态失败：注入前几次失败，重试后成功（效果一次）；
//   - 永久失败 / 非法负载：不重试进入死信，保留可区分的失败原因。
//
// 运行（不需要任何外部中间件或网络）：
//
//	go run ./cmd/demo -dir ./.demo-data -clean
//
// 去掉 -clean 可观察重启恢复：再次运行会从持久化水位继续。
package main

import (
	"context"
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

	// fail-2 前 2 次处理失败（瞬态），第 3 次成功；perm 永久失败。
	h := pipeline.NewCountHandler().
		FailNTimes("fail-2", 2).
		FailPermanent("perm", "demo: business rejects this event")

	cfg := pipeline.DefaultConfig(*dir)
	cfg.RetryBackoff = 50 * time.Millisecond
	cfg.MaxRetryBackoff = 500 * time.Millisecond
	cfg.Logger = pipeline.NewLogger(os.Stdout)

	p, err := pipeline.Open(cfg, h)
	if err != nil {
		fatal("open: %v", err)
	}

	events := []pipeline.Event{
		{ID: "add-1", Key: "tenant-a", Type: "inc", Payload: []byte("orders")},
		{ID: "add-1", Key: "tenant-a", Type: "inc", Payload: []byte("orders")}, // 重复投递
		{ID: "add-2", Key: "tenant-b", Type: "add", Payload: []byte("orders=10")},
		{ID: "fail-2", Key: "tenant-a", Type: "inc", Payload: []byte("retries")}, // 失败重试
		{ID: "perm", Key: "tenant-b", Type: "inc", Payload: []byte("rejected")},  // 永久死信
		{ID: "bad", Key: "tenant-a", Type: "add", Payload: []byte("garbage")},    // 非法负载死信
		{ID: "add-3", Key: "tenant-a", Type: "inc", Payload: []byte("orders")},
	}

	ctx := context.Background()
	if err := p.Start(ctx); err != nil {
		fatal("start: %v", err)
	}
	for _, ev := range events {
		off, err := p.Publish(ev)
		if err != nil {
			fatal("publish %s: %v", ev.ID, err)
		}
		fmt.Printf(">>> published event=%s offset=%d key=%s\n", ev.ID, off, ev.Key)
	}

	wait, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := p.WaitForOffset(wait, int64(len(events))); err != nil {
		fatal("wait: %v", err)
	}
	if err := p.Drain(ctx); err != nil {
		fatal("drain: %v", err)
	}

	printState(p, "final state after processing")

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
	fmt.Println("rerun without -clean to observe restart recovery from the persisted watermark")
}

func printState(p *pipeline.Pipeline, title string) {
	snap := p.Snapshot()
	kv := snap.Business.(*pipeline.KVState)
	fmt.Printf("\n=== %s ===\n", title)
	fmt.Printf("committed offset: %d / next: %d\n", snap.CommittedOffset, p.Stats().NextOffset)
	fmt.Printf("business effects : %v\n", kv.Values)
	fmt.Printf("dead letters (%d):\n", len(snap.Dead))
	for _, d := range snap.Dead {
		fmt.Printf("  - event=%-6s offset=%d kind=%-9s attempts=%d reason=%q\n",
			d.EventID, d.Offset, d.Kind, d.Attempts, d.Reason)
	}
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "demo failed: "+format+"\n", args...)
	os.Exit(1)
}
