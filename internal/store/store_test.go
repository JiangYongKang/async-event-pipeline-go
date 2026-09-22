package store

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

func TestPublishIdempotentAndReopen(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	s, err := Open(ctx, filepath.Join(dir, "st"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	e1, dup1, err := s.Publish(ctx, "acct-A", "evt-1", "account.deposit", []byte(`{"amount":10}`))
	if err != nil || dup1 {
		t.Fatalf("first publish: dup=%v err=%v", dup1, err)
	}
	if e1.KeySeq != 1 || e1.GlobalSeq != 1 {
		t.Fatalf("unexpected seqs: %+v", e1)
	}

	// Same ID on a different key must not append a second record.
	e1b, dup2, err := s.Publish(ctx, "acct-B", "evt-1", "x", nil)
	if err != nil {
		t.Fatalf("dup publish: %v", err)
	}
	if !dup2 || e1b.Key != "acct-A" || e1b.KeySeq != 1 {
		t.Fatalf("dup publish returned wrong envelope: %+v dup=%v", e1b, dup2)
	}

	e2, _, err := s.Publish(ctx, "acct-A", "evt-2", "account.withdraw", nil)
	if err != nil {
		t.Fatalf("publish2: %v", err)
	}
	if e2.KeySeq != 2 || e2.GlobalSeq != 2 {
		t.Fatalf("unexpected seqs: %+v", e2)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopen: WAL and state must reconcile exactly.
	s2, err := Open(ctx, s.dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	if got := s2.GlobalSeqForTest(); got != 2 {
		t.Fatalf("global seq after reopen = %d, want 2", got)
	}
	pending := s2.Pending("acct-A", 0)
	if len(pending) != 2 || pending[0].ID != "evt-1" || pending[1].ID != "evt-2" {
		t.Fatalf("pending order wrong: %+v", pending)
	}
	if _, dup, err := s2.Publish(ctx, "acct-A", "evt-1", "x", nil); err != nil || !dup {
		t.Fatalf("dedup after reopen failed: dup=%v err=%v", dup, err)
	}
}

func TestCommitAtomicityAndCheckpointResume(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "st")
	ctx := context.Background()
	s, err := Open(ctx, dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	env, _, err := s.Publish(ctx, "k", "e1", "t", nil)
	if err != nil {
		t.Fatal(err)
	}
	mut := Mutation{Puts: map[string][]byte{"balance": []byte("100")}}
	if err := s.Commit(ctx, env, mut, nil); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if v, err := s.Get("balance"); err != nil || string(v) != "100" {
		t.Fatalf("committed state wrong: %q %v", v, err)
	}
	if s.View().Checkpoints["k"] != 1 {
		t.Fatalf("checkpoint not advanced")
	}
	s.Close()

	// Reopen: checkpoint survives, so Pending from the acked position is empty.
	s2, err := Open(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if s2.View().Checkpoints["k"] != 1 {
		t.Fatalf("checkpoint lost across restart")
	}
	if got := s2.Pending("k", s2.View().Checkpoints["k"]); len(got) != 0 {
		t.Fatalf("acked event redelivered: %+v", got)
	}
	if v, err := s2.Get("balance"); err != nil || string(v) != "100" {
		t.Fatalf("state lost across restart: %q %v", v, err)
	}
}

func TestCommitOutOfOrderRejected(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "st")
	ctx := context.Background()
	s, _ := Open(ctx, dir)
	defer s.Close()

	e1, _, _ := s.Publish(ctx, "k", "e1", "t", nil)
	e2, _, _ := s.Publish(ctx, "k", "e2", "t", nil)
	// Commit seq 2 while checkpoint is 0: must be rejected with no side effect.
	if err := s.Commit(ctx, e2, Mutation{Puts: map[string][]byte{"x": []byte("y")}}, nil); err == nil {
		t.Fatal("expected out-of-order rejection")
	}
	if _, err := s.Get("x"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("partial state visible after rejected commit: %v", err)
	}
	if err := s.Commit(ctx, e1, Mutation{}, nil); err != nil {
		t.Fatalf("commit e1: %v", err)
	}
}

func TestDeadLetterDurableAndDistinguishable(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "st")
	ctx := context.Background()
	s, _ := Open(ctx, dir)

	env, _, _ := s.Publish(ctx, "k", "bad-1", "t", nil)
	rec := &DeadRecord{Attempts: 3, Reason: "INSUFFICIENT_FUNDS", Fatal: false}
	if err := s.Commit(ctx, env, Mutation{Skip: true}, rec); err != nil {
		t.Fatal(err)
	}
	s.Close()

	s2, _ := Open(ctx, dir)
	defer s2.Close()
	dl := s2.DeadLetters()
	if len(dl) != 1 || dl[0].Reason != "INSUFFICIENT_FUNDS" || dl[0].Attempts != 3 || dl[0].Fatal != false {
		t.Fatalf("dead letter not reconstructed correctly: %+v", dl)
	}
	if s2.View().Checkpoints["k"] != 1 {
		t.Fatalf("dead-lettered event should advance the checkpoint")
	}
}

func TestAdvanceRejectsBackwardAndOutOfRange(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "st")
	ctx := context.Background()
	s, _ := Open(ctx, dir)
	defer s.Close()

	_, _, _ = s.Publish(ctx, "k", "e1", "t", nil)
	_, _, _ = s.Publish(ctx, "k", "e2", "t", nil)

	if err := s.Advance(ctx, "k", 1, "seek"); err != nil {
		t.Fatalf("forward advance: %v", err)
	}
	if err := s.Advance(ctx, "k", 1, "seek"); !errors.Is(err, ErrSeekBackwards) {
		t.Fatalf("want ErrSeekBackwards, got %v", err)
	}
	if err := s.Advance(ctx, "k", 5, "seek"); !errors.Is(err, ErrSeekOutOfRange) {
		t.Fatalf("want ErrSeekOutOfRange, got %v", err)
	}
}

// GlobalSeqForTest exposes the allocated global counter for assertions.
func (s *Store) GlobalSeqForTest() uint64 {
	return s.View().GlobalSeq
}
