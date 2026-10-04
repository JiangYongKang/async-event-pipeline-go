// Command demo 在本地数据目录上跑通一条异步事件投递与消费链路。
//
// 它演示：
//   - 正常事件：恰好生效一次；
//   - 重复投递：相同事件 ID 多次投递，业务效果只生效一次；
//   - 瞬态失败：注入前几次失败，重试后成功（效果一次）；
//   - 永久失败 / 非法负载：不重试进入死信，保留可区分的失败原因；
//   - 完成判定：WaitUntilIdle 等到“真正处理完”，随后打印可解释的进度说明；
//   - 复用目录：第二次运行（不带 -clean）在旧进度上追加全新一批事件，
//     新事件与历史事件享有同样的可靠投递、幂等与同键顺序保证。
//
// 运行（不需要任何外部中间件或网络）：
//
//	go run ./cmd/demo -dir ./.demo-data -clean      # 首跑：清空目录
//	go run ./cmd/demo -dir ./.demo-data             # 复用目录，追加新批次
//	go run ./cmd/demo -dir ./.demo-data             # 再次复用，重投同 ID（跨重启去重，计数不增长）
//	go run ./cmd/demo -dir ./.demo-data -replay     # 停机后显式全量重放
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

	h := pipeline.NewCountHandler().
		FailNTimes("fail-2", 2). // 首批：前 2 次失败，第 3 次成功
		FailPermanent("perm", "demo: business rejects this event").
		FailNTimes("new-flaky", 2) // 复用目录追加批次里的重试事件

	cfg := pipeline.DefaultConfig(*dir)
	cfg.RetryBackoff = 50 * time.Millisecond
	cfg.MaxRetryBackoff = 500 * time.Millisecond
	cfg.Logger = pipeline.NewLogger(os.Stdout)

	p, err := pipeline.Open(cfg, h)
	if err != nil {
		fatal("open: %v", err)
	}

	// 打开后、Start 前探测历史进度，决定本次要发布哪一批事件。
	existing := p.Stats().NextOffset
	ctx := context.Background()
	if err := p.Start(ctx); err != nil {
		fatal("start: %v", err)
	}

	var events []pipeline.Event
	if existing == 0 {
		fmt.Println(">>> fresh directory: publishing initial batch")
		events = []pipeline.Event{
			{ID: "add-1", Key: "tenant-a", Type: "inc", Payload: []byte("orders")},
			{ID: "add-1", Key: "tenant-a", Type: "inc", Payload: []byte("orders")}, // 重复投递
			{ID: "add-2", Key: "tenant-b", Type: "add", Payload: []byte("orders=10")},
			{ID: "fail-2", Key: "tenant-a", Type: "inc", Payload: []byte("retries")}, // 失败重试
			{ID: "perm", Key: "tenant-b", Type: "inc", Payload: []byte("rejected")},  // 永久死信
			{ID: "bad", Key: "tenant-a", Type: "add", Payload: []byte("garbage")},    // 非法负载死信
			{ID: "add-3", Key: "tenant-a", Type: "inc", Payload: []byte("orders")},
		}
	} else {
		fmt.Printf(">>> reusing directory: resuming from committed history (logEnd=%d), appending NEW batch\n", existing)
		events = []pipeline.Event{
			{ID: "new-1", Key: "tenant-a", Type: "inc", Payload: []byte("orders")},      // 与历史同键，顺序衔接
			{ID: "new-flaky", Key: "tenant-c", Type: "inc", Payload: []byte("retries")}, // 新批次里的重试事件
			{ID: "add-1", Key: "tenant-a", Type: "inc", Payload: []byte("orders")},      // 跨重启重复 ID，不二次生效
			{ID: "new-2", Key: "tenant-c", Type: "inc", Payload: []byte("orders")},
		}
	}
	for _, ev := range events {
		off, err := p.Publish(ev)
		if err != nil {
			fatal("publish %s: %v", ev.ID, err)
		}
		fmt.Printf(">>> published event=%s offset=%d key=%s\n", ev.ID, off, ev.Key)
	}

	// 关键动作：等到“真正处理完”再继续——含运行中追加、重试中、dispatcher
	// 尚未读到的事件。返回后看到的状态必定完整。
	wait, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := p.WaitUntilIdle(wait); err != nil {
		fatal("wait-until-idle: %v\n%s", err, p.Progress().String())
	}
	if err := p.Drain(ctx); err != nil {
		fatal("drain: %v", err)
	}

	printState(p, "final state after WaitUntilIdle + Drain")

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
	fmt.Println("rerun without -clean to resume and append a new batch onto the same directory")
}

func printState(p *pipeline.Pipeline, title string) {
	pr := p.Progress()
	snap := p.Snapshot()
	kv := snap.Business.(*pipeline.KVState)
	fmt.Printf("\n=== %s ===\n", title)
	fmt.Print(pr.String())
	fmt.Printf("business effects : %v\n", kv.Values)
	fmt.Printf("dead letters (%d):\n", len(snap.Dead))
	for _, d := range snap.Dead {
		fmt.Printf("  - event=%-9s offset=%d kind=%-9s attempts=%d reason=%q\n",
			d.EventID, d.Offset, d.Kind, d.Attempts, d.Reason)
	}
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "demo failed: "+format+"\n", args...)
	os.Exit(1)
}
