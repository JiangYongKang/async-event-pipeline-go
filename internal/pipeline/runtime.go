package pipeline

import (
	"math"
	"time"

	"github.com/highcumontoa/async-event-pipeline-go/internal/event"
	"github.com/highcumontoa/async-event-pipeline-go/internal/store"
)

// dispatchLoop owns scheduling: it starts at most one worker per ordering key,
// bounded by a global concurrency semaphore. Workers are retriggered by wake
// signals from publish/seek/commit and by a short poll timer.
func (p *Pipeline) dispatchLoop() {
	defer p.wg.Done()

	sem := make(chan struct{}, p.cfg.Concurrency)
	timer := time.NewTicker(p.cfg.PollInterval)
	defer timer.Stop()

	tick := func() {
		p.runMu.Lock()
		defer p.runMu.Unlock()
		snap := p.st.View()
		for _, key := range p.st.Keys() {
			if p.running[key] {
				continue
			}
			pending := p.st.Pending(key, snap.Checkpoints[key])
			if len(pending) == 0 {
				continue
			}
			select {
			case sem <- struct{}{}:
			default:
				// Parallelism budget exhausted; remaining keys wait for a
				// worker to finish and the next dispatch tick.
				return
			}
			p.running[key] = true
			p.wg.Add(1)
			go p.runKey(key, sem)
		}
	}

	tick()
	for {
		select {
		case <-p.ctx.Done():
			return
		case <-timer.C:
			tick()
		case <-p.wake:
			tick()
		}
	}
}

// runKey processes one ordering key's events strictly in KeySeq order until
// the key drains or the runtime shuts down. One key = one goroutine at a
// time, so ordering on a key is structural; different keys run in parallel.
func (p *Pipeline) runKey(key string, sem chan struct{}) {
	defer func() {
		<-sem
		p.runMu.Lock()
		p.running[key] = false
		p.inflight--
		if p.inflight < 0 {
			p.inflight = 0
		}
		p.runMu.Unlock()
		p.signal()
		p.wg.Done()
	}()

	p.runMu.Lock()
	p.inflight++
	p.runMu.Unlock()

	for {
		if p.ctx.Err() != nil {
			return
		}
		snap := p.st.View()
		pending := p.st.Pending(key, snap.Checkpoints[key])
		if len(pending) == 0 {
			return
		}
		env := pending[0]

		// Defensive dedup: the dedup marker and checkpoint commit atomically,
		// so a redelivery of an already-processed ID reaching this point would
		// mean the store invariants were broken. Log it loudly rather than
		// silently applying effects twice or silently skipping ahead.
		if done := snap.Processed[key][env.ID]; done {
			p.log.Error("dedup invariant anomaly: event marked processed before checkpoint",
				"event_id", env.ID, "key", key, "key_seq", env.KeySeq,
				"progress", snap.Checkpoints[key], "decision", "halt_key")
			return
		}

		cont := p.processOne(env)
		p.announce()
		if !cont {
			return
		}
	}
}

// processOne executes one event with its retry policy. It returns false when
// the key worker should stop (shutdown or an undrivable failure); a
// dead-lettered event still returns true because its checkpoint advanced and
// the next ordered event must be processed.
func (p *Pipeline) processOne(env store.Envelope) (cont bool) {
	for {
		if p.ctx.Err() != nil {
			return false
		}
		attempt := p.attemptInc(env.ID)
		d := event.Delivery{Event: toEvent(env), Attempt: attempt}
		tx := newTx(p.st.View().State)

		err := p.proc(p.ctx, tx, d)
		snap := p.st.View()
		if err == nil {
			mut := store.Mutation{Puts: tx.puts, Deletes: tx.deletes, Skip: tx.skip}
			if cerr := p.commitWithRetry(env, mut, nil); cerr != nil {
				p.log.Error("commit failed, stopping key worker",
					"event_id", env.ID, "key", env.Key, "key_seq", env.KeySeq, "err", cerr.Error())
				return false
			}
			p.attemptDone(env.ID)
			p.log.Info("event committed",
				"event_id", env.ID, "key", env.Key, "key_seq", env.KeySeq,
				"attempt", attempt, "progress_before", snap.Checkpoints[env.Key],
				"progress_after", env.KeySeq, "skipped", tx.skip,
				"decision", "commit")
			return true
		}

		kind := classify(err)
		fatal := kind == kindFatal
		exhausted := attempt >= p.cfg.MaxAttempts
		if fatal || exhausted {
			rec := &store.DeadRecord{
				Attempts: attempt,
				Reason:   err.Error(),
				Fatal:    fatal,
			}
			if cerr := p.commitWithRetry(env, store.Mutation{Skip: true}, rec); cerr != nil {
				p.log.Error("dead-letter commit failed, stopping key worker",
					"event_id", env.ID, "key", env.Key, "err", cerr.Error())
				return false
			}
			p.attemptDone(env.ID)
			p.log.Error("event dead-lettered",
				"event_id", env.ID, "key", env.Key, "key_seq", env.KeySeq,
				"attempts", attempt, "progress_after", env.KeySeq,
				"fatal", fatal, "reason", err.Error(),
				"decision", map[bool]string{true: "dead_fatal", false: "dead_retry_exhausted"}[fatal])
			return true
		}

		wait := p.backoff(attempt)
		p.log.Error("event failed, scheduling retry",
			"event_id", env.ID, "key", env.Key, "key_seq", env.KeySeq,
			"attempt", attempt, "max_attempts", p.cfg.MaxAttempts,
			"progress", snap.Checkpoints[env.Key], "backoff_ms", wait.Milliseconds(),
			"reason", err.Error(), "decision", "retry")
		select {
		case <-p.ctx.Done():
			return false
		case <-time.After(wait):
		}
	}
}

// commitWithRetry retries only the durable commit step. Processor effects are
// buffered in tx and never partially visible, so re-committing cannot apply
// an effect twice: a failed commit leaves the checkpoint untouched and the
// same mutation is presented again.
func (p *Pipeline) commitWithRetry(env store.Envelope, mut store.Mutation, dead *store.DeadRecord) error {
	var lastErr error
	for i := 0; ; i++ {
		err := p.st.Commit(p.ctx, env, mut, dead)
		if err == nil {
			return nil
		}
		lastErr = err
		if p.ctx.Err() != nil {
			return p.ctx.Err()
		}
		p.log.Error("state commit error",
			"event_id", env.ID, "key", env.Key, "try", i+1, "err", err.Error())
		select {
		case <-p.ctx.Done():
			return p.ctx.Err()
		case <-time.After(p.backoff(i + 1)):
		}
		_ = lastErr
	}
}

func (p *Pipeline) backoff(attempt int) time.Duration {
	d := float64(p.cfg.BackoffInitial) * math.Pow(2, float64(attempt-1))
	if d > float64(p.cfg.BackoffMax) {
		d = float64(p.cfg.BackoffMax)
	}
	return time.Duration(d)
}

func (p *Pipeline) attemptInc(id string) int {
	p.runMu.Lock()
	defer p.runMu.Unlock()
	p.attempts[id]++
	return p.attempts[id]
}

func (p *Pipeline) attemptDone(id string) {
	p.runMu.Lock()
	defer p.runMu.Unlock()
	delete(p.attempts, id)
}

func (p *Pipeline) announce() {
	select {
	case p.notifyCh <- struct{}{}:
	default:
	}
}
