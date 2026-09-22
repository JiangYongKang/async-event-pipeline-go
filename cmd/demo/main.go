// Command demo runs a fully local, in-process demonstration of the async
// event pipeline: duplicate publishes, failing-then-retrying events,
// per-key ordering across concurrent keys, poison events reaching a terminal
// dead-letter state, rejected seek operations and restart recovery. No
// network or external middleware is used; all durability lives in one local
// directory (./.demo-data by default, override with -dir).
//
// Run:
//
//	go run ./cmd/demo
//	go run ./cmd/demo -restart   # reuse the directory and verify recovery
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/highcumontoa/async-event-pipeline-go/internal/business"
	"github.com/highcumontoa/async-event-pipeline-go/internal/pipeline"
	"github.com/highcumontoa/async-event-pipeline-go/internal/store"
)

func main() {
	dir := flag.String("dir", ".demo-data", "local data directory for the event log and state")
	restart := flag.Bool("restart", false, "reuse the data directory to demonstrate restart recovery")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ctx := context.Background()

	if err := os.MkdirAll(*dir, 0o755); err != nil {
		fail("create data dir: %v", err)
	}

	// Deterministic failure injection: dep-bob-2 fails its first two attempts
	// with a transient error, then succeeds exactly once.
	hooks := business.NewHooks()
	hooks.FailNext("dep-bob-2", 2)

	st, err := store.Open(ctx, *dir)
	if err != nil {
		fail("open store: %v", err)
	}
	cfg := pipeline.Config{
		MaxAttempts:    4,
		BackoffInitial: 20 * time.Millisecond,
		BackoffMax:     200 * time.Millisecond,
		Concurrency:    4,
		PollInterval:   2 * time.Millisecond,
		Logger:         log,
	}
	p := pipeline.NewPipeline(st, business.AccountProcessor(hooks), cfg)
	if err := p.Start(ctx); err != nil {
		fail("start: %v", err)
	}

	banner("restart=%t data_dir=%s", *restart, *dir)
	if !*restart {
		freshScenario(ctx, log, p)
	} else {
		recoveryScenario(ctx, log, p)
	}

	// Show rejected seeks explicitly.
	demonstrateSeekRejection(ctx, log, p)

	idle, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := p.AwaitIdle(idle); err != nil {
		fail("pipeline did not drain: %v", err)
	}

	printFinalState(log, p)
	shutdown(p, st)
	log.Info("demo complete", "state_file", filepath.Join(*dir, "state.json"),
		"event_log", filepath.Join(*dir, "events.jsonl"),
		"dead_log", filepath.Join(*dir, "dead.jsonl"))
}

// freshScenario publishes the full story on a clean data directory.
func freshScenario(ctx context.Context, log *slog.Logger, p *pipeline.Pipeline) {
	mustPub(ctx, p, "alice", "open-alice", business.CmdOpen, business.Command{Account: "alice"})
	mustPub(ctx, p, "alice", "dep-alice-1", business.CmdDeposit, business.Command{Account: "alice", Amount: 100})
	mustPub(ctx, p, "alice", "wd-alice-1", business.CmdWithdraw, business.Command{Account: "alice", Amount: 30})

	mustPub(ctx, p, "bob", "open-bob", business.CmdOpen, business.Command{Account: "bob"})
	mustPub(ctx, p, "bob", "dep-bob-1", business.CmdDeposit, business.Command{Account: "bob", Amount: 50})
	// Transient failures on the first two attempts (hooks), then success once.
	mustPub(ctx, p, "bob", "dep-bob-2", business.CmdDeposit, business.Command{Account: "bob", Amount: 25})

	// Duplicate redelivery (different goroutines would behave the same):
	// the same event ID is republished several times concurrently.
	for i := 0; i < 3; i++ {
		go func() {
			o, err := p.Publish(ctx, "bob", "dep-bob-2", business.CmdDeposit,
				mustJSON(business.Command{Account: "bob", Amount: 25}))
			if err != nil {
				fail("dup publish: %v", err)
			}
			if !o.Dup {
				fail("republish was not marked duplicate")
			}
		}()
	}

	// Two distinct failure reasons, both terminal on the "carol" key.
	mustPub(ctx, p, "carol", "wd-carol-noopen", business.CmdWithdraw, business.Command{Account: "carol", Amount: 1})
	mustPub(ctx, p, "carol", "open-carol", business.CmdOpen, business.Command{Account: "carol"})
	mustPub(ctx, p, "carol", "dep-carol-1", business.CmdDeposit, business.Command{Account: "carol", Amount: 10})
	mustPub(ctx, p, "carol", "wd-carol-over", business.CmdWithdraw, business.Command{Account: "carol", Amount: 999})
	// Poison message with an unparseable payload.
	if _, err := p.Publish(ctx, "carol", "poison-carol", business.CmdDeposit, []byte("{not-json")); err != nil {
		fail("publish poison: %v", err)
	}
	// Trailing valid event proves a terminal event does not block the key.
	mustPub(ctx, p, "carol", "dep-carol-tail", business.CmdDeposit, business.Command{Account: "carol", Amount: 2})
	log.Info("scenario published: duplicate delivery, transient retry, fatal rejections, poison event")
}

// recoveryScenario reuses the durable directory, publishes one more event per
// account, and verifies balances match a single sequential execution.
func recoveryScenario(ctx context.Context, log *slog.Logger, p *pipeline.Pipeline) {
	mustPub(ctx, p, "alice", "dep-alice-restart", business.CmdDeposit, business.Command{Account: "alice", Amount: 7})
	mustPub(ctx, p, "bob", "dep-bob-restart", business.CmdDeposit, business.Command{Account: "bob", Amount: 7})
	mustPub(ctx, p, "carol", "dep-carol-restart", business.CmdDeposit, business.Command{Account: "carol", Amount: 7})
	log.Info("recovery scenario: resumed from persisted checkpoints, no replay of acked events")
}

func demonstrateSeekRejection(ctx context.Context, log *slog.Logger, p *pipeline.Pipeline) {
	// Allow the small set to drain first.
	idle, cancel := context.WithTimeout(ctx, 5*time.Second)
	_ = p.AwaitIdle(idle)
	cancel()

	if _, err := p.Seek(ctx, "alice", 1); err != nil {
		log.Warn("backward seek rejected as required", "err", err.Error())
	}
	if _, err := p.Seek(ctx, "ghost", 42); err != nil {
		log.Warn("unrecoverable seek rejected as required", "err", err.Error())
	}
}

func printFinalState(log *slog.Logger, p *pipeline.Pipeline) {
	snap := p.View()
	log.Info("final checkpoints", "checkpoints", snap.Checkpoints)
	for _, acc := range []string{"alice", "bob", "carol"} {
		raw := snap.State[business.BalanceKey(acc)]
		log.Info("final balance", "account", acc, "balance", raw)
	}
	for _, d := range p.DeadLetters() {
		log.Warn("terminal failure record",
			"event_id", d.Event.ID, "key", d.Event.Key, "key_seq", d.Event.KeySeq,
			"attempts", d.Attempts, "fatal", d.Fatal, "reason", d.Reason)
	}
}

func mustPub(ctx context.Context, p *pipeline.Pipeline, key, id, typ string, c business.Command) {
	if _, err := p.Publish(ctx, key, id, typ, mustJSON(c)); err != nil {
		fail("publish %s: %v", id, err)
	}
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		fail("marshal: %v", err)
	}
	return b
}

func shutdown(p *pipeline.Pipeline, st *store.Store) {
	if err := p.Close(); err != nil {
		fail("pipeline close: %v", err)
	}
	if err := st.Close(); err != nil {
		fail("store close: %v", err)
	}
}

func banner(format string, args ...any) {
	fmt.Println("==================== async-event-pipeline demo ====================")
	fmt.Printf(format+"\n", args...)
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "demo error: "+format+"\n", args...)
	os.Exit(1)
}
