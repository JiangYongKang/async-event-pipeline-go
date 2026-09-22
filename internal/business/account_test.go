package business_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/highcumontoa/async-event-pipeline-go/internal/business"
	"github.com/highcumontoa/async-event-pipeline-go/internal/pipeline"
	"github.com/highcumontoa/async-event-pipeline-go/internal/store"
)

func startDemo(t *testing.T, hooks business.FlakyHooks, cfg pipeline.Config) (*pipeline.Pipeline, *store.Store) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "data")
	st, err := store.Open(context.Background(), dir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	p := pipeline.NewPipeline(st, business.AccountProcessor(hooks), cfg)
	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { _ = p.Close(); _ = st.Close() })
	return p, st
}

func cmdPayload(t *testing.T, c business.Command) []byte {
	t.Helper()
	b, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func publish(t *testing.T, p *pipeline.Pipeline, key, id, typ string, c business.Command) {
	t.Helper()
	payload := cmdPayload(t, c)
	if _, err := p.Publish(context.Background(), key, id, typ, payload); err != nil {
		t.Fatalf("publish %s: %v", id, err)
	}
}

func await(t *testing.T, p *pipeline.Pipeline) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := p.AwaitIdle(ctx); err != nil {
		t.Fatalf("await idle: %v", err)
	}
}

func mustBalance(t *testing.T, p *pipeline.Pipeline, account string) int64 {
	t.Helper()
	raw, ok := p.View().State[business.BalanceKey(account)]
	if !ok {
		return 0
	}
	v, err := business.ParseBalance(raw)
	if err != nil {
		t.Fatalf("parse balance %q: %v", raw, err)
	}
	return v
}

func findDead(t *testing.T, p *pipeline.Pipeline, id string) store.DeadRecord {
	t.Helper()
	for _, d := range p.DeadLetters() {
		if d.Event.ID == id {
			return d
		}
	}
	t.Fatalf("dead letter %s not found", id)
	return store.DeadRecord{}
}

func TestAccountHappyPathWithTransientRetryNoDoubleEffect(t *testing.T) {
	hooks := business.NewHooks()
	// The deposit fails its first two attempts, then succeeds: the balance
	// must be credited exactly once.
	hooks.FailNext("dep-1", 2)
	p, _ := startDemo(t, hooks, pipeline.Config{MaxAttempts: 5, BackoffInitial: time.Millisecond})

	publish(t, p, "acct-1", "open-1", business.CmdOpen, business.Command{Account: "acct-1"})
	publish(t, p, "acct-1", "dep-1", business.CmdDeposit, business.Command{Account: "acct-1", Amount: 100})
	// Duplicate redelivery of the same event ID (same and different caller key).
	if _, err := p.Publish(context.Background(), "acct-1", "dep-1", business.CmdDeposit,
		cmdPayload(t, business.Command{Account: "acct-1", Amount: 100})); err != nil {
		t.Fatal(err)
	}
	await(t, p)

	if got := mustBalance(t, p, "acct-1"); got != 100 {
		t.Fatalf("balance=%d, want 100 (retried deposit must apply exactly once)", got)
	}
	if attempts := hooks.Attempts("dep-1"); attempts != 3 {
		t.Fatalf("dep-1 attempts=%d, want 3", attempts)
	}
	t.Logf("判定依据: event_id=dep-1 处理进度 attempts=1..%d, 最终 balance=100, 重复投递 dup 未生效", hooks.Attempts("dep-1"))
}

func TestAccountDistinguishableFailureReasons(t *testing.T) {
	p, _ := startDemo(t, nil, pipeline.Config{MaxAttempts: 3, BackoffInitial: time.Millisecond})

	// Withdraw before open: ACCOUNT_NOT_OPEN (fatal, blocks nothing later).
	publish(t, p, "acct-X", "wd-before", business.CmdWithdraw, business.Command{Account: "acct-X", Amount: 10})
	publish(t, p, "acct-X", "open-x", business.CmdOpen, business.Command{Account: "acct-X"})
	// Open twice: ALREADY_OPEN.
	publish(t, p, "acct-X", "open-again", business.CmdOpen, business.Command{Account: "acct-X"})
	// Deposit then overdraw: INSUFFICIENT_FUNDS.
	publish(t, p, "acct-X", "dep-x", business.CmdDeposit, business.Command{Account: "acct-X", Amount: 5})
	publish(t, p, "acct-X", "wd-over", business.CmdWithdraw, business.Command{Account: "acct-X", Amount: 100})
	// Poison payload: BAD_COMMAND.
	if _, err := p.Publish(context.Background(), "acct-X", "poison", business.CmdDeposit, []byte("{not-json")); err != nil {
		t.Fatal(err)
	}
	// A healthy trailing event proves poison messages don't block the key.
	publish(t, p, "acct-X", "dep-ok", business.CmdDeposit, business.Command{Account: "acct-X", Amount: 1})
	await(t, p)

	cases := []struct{ id, reason string }{
		{"wd-before", business.ReasonNotOpen},
		{"open-again", business.ReasonAlreadyOpen},
		{"wd-over", business.ReasonInsufficient},
		{"poison", business.ReasonBadCommand},
	}
	for _, c := range cases {
		d := findDead(t, p, c.id)
		if !d.Fatal {
			t.Fatalf("%s: expected fatal dead letter, got %+v", c.id, d)
		}
		if !containsStr(d.Reason, c.reason) {
			t.Fatalf("%s: reason=%q, want it to contain %q", c.id, d.Reason, c.reason)
		}
		t.Logf("判定依据: event_id=%s 终态=dead(fatal) reason 区分码=%s", c.id, c.reason)
	}
	if got := mustBalance(t, p, "acct-X"); got != 6 {
		t.Fatalf("balance=%d, want 6 (5+1; overdraft rejected, later event still processed)", got)
	}
	if p.View().Checkpoints["acct-X"] != 7 {
		t.Fatalf("checkpoint=%d, want 7 (all events acked incl. dead-lettered)", p.View().Checkpoints["acct-X"])
	}
}

func TestAccountCrossKeyConcurrencyAndRestart(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	open := func() (*pipeline.Pipeline, *store.Store) {
		st, err := store.Open(context.Background(), dir)
		if err != nil {
			t.Fatal(err)
		}
		p := pipeline.NewPipeline(st, business.AccountProcessor(nil), pipeline.Config{
			Concurrency:    8,
			BackoffInitial: time.Millisecond,
			Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		})
		if err := p.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		return p, st
	}

	p, st := open()
	accounts := []string{"alice", "bob", "carol", "dave"}
	var wg sync.WaitGroup
	for _, acc := range accounts {
		wg.Add(1)
		go func(acc string) {
			defer wg.Done()
			publish(t, p, acc, acc+"-open", business.CmdOpen, business.Command{Account: acc})
			for i := 1; i <= 10; i++ {
				publish(t, p, acc, acc+"-d"+itoa(i), business.CmdDeposit,
					business.Command{Account: acc, Amount: 10})
			}
		}(acc)
	}
	wg.Wait()
	await(t, p)
	_ = p.Close()
	_ = st.Close()

	// Restart: every balance must be exactly 100 regardless of scheduling.
	p2, st2 := open()
	for _, acc := range accounts {
		publish(t, p2, acc, acc+"-final", business.CmdDeposit, business.Command{Account: acc, Amount: 5})
	}
	await(t, p2)
	for _, acc := range accounts {
		if got := mustBalance(t, p2, acc); got != 105 {
			t.Fatalf("%s balance=%d, want 105 (restart replay equals sequential result)", acc, got)
		}
	}
	t.Logf("判定依据: 4 个账户并发存款后重启再存 5; 每账户 balance=105, 无丢失/重复/串键")
	_ = p2.Close()
	_ = st2.Close()
}

func containsStr(s, sub string) bool {
	return len(s) >= len(sub) && indexOf(s, sub) >= 0
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
