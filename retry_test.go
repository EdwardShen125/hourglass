package hourglass

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestRetryPolicyBackoffReschedulesFailedHandler(t *testing.T) {
	e, clk := newTestEngine(t)
	ctx := context.Background()

	var attempts atomic.Int32
	e.Register("flaky", func(context.Context, Job) error {
		if attempts.Add(1) < 3 {
			return errTestBoom // fails twice, succeeds third
		}
		return nil
	})

	id, err := e.Submit(ctx, "flaky", nil,
		At(clk.Now().Add(5*time.Second)), Retries(5), RetryBackoff(time.Minute))
	if err != nil {
		t.Fatal(err)
	}

	// fire 1 at t0+5s (t0 = 09:00:00, so fire at 09:00:05)
	e.step(ctx)
	e.pump(clk.Now().Add(5 * time.Second))
	waitJobState(t, e, id, Waiting) // backoff re-queued, not dead

	// retry due = fire time + backoff = 09:01:05; advance past it
	clk.Advance(65 * time.Second) // now 09:01:05
	e.step(ctx)
	e.pump(clk.Now())
	waitAttempts(t, e, id, 2)

	// second failure: retry due = 09:02:05
	clk.Advance(time.Minute)
	e.step(ctx)
	e.pump(clk.Now())
	waitJobState(t, e, id, Completed)

	j, _ := e.Get(ctx, id)
	if j.State != Completed {
		t.Fatalf("final state = %s, want completed after 3 attempts", j.State)
	}
	if attempts.Load() != 3 {
		t.Errorf("handler ran %d times, want 3", attempts.Load())
	}
}

func TestRetryPolicyExhaustionDeadLetters(t *testing.T) {
	e, clk := newTestEngine(t)
	ctx := context.Background()

	var attempts atomic.Int32
	e.Register("boom", func(context.Context, Job) error {
		attempts.Add(1)
		return errTestBoom
	})

	id, _ := e.Submit(ctx, "boom", nil,
		At(clk.Now().Add(5*time.Second)), Retries(1), RetryBackoff(10*time.Second))

	// attempt 1 at 09:00:05
	e.step(ctx)
	e.pump(clk.Now().Add(5 * time.Second))
	waitAttempts(t, e, id, 1)
	// attempt 2 at 09:00:15 (fire1 at 09:00:05 + backoff 10s;
	// Retries(1) = 2 total attempts). Advance to the due, not just the
	// backoff length.
	clk.Advance(10*time.Second + 5*time.Second)
	e.step(ctx)
	e.pump(clk.Now())
	waitJobState(t, e, id, Dead)

	if attempts.Load() != 2 {
		t.Errorf("handler ran %d times, want 2 (1 + 1 retry)", attempts.Load())
	}
	j, _ := e.Get(ctx, id)
	if j.State != Dead {
		t.Fatalf("state = %s, want dead after exhaustion", j.State)
	}
	// dead is terminal: no further reschedule even after time passes
	clk.Advance(time.Minute)
	e.step(ctx)
	e.pump(clk.Now())
	if attempts.Load() != 2 {
		t.Errorf("dead job retried: %d attempts", attempts.Load())
	}
}

func TestRetryAttemptCountOnReceipts(t *testing.T) {
	e, clk := newTestEngine(t)
	ctx := context.Background()
	var attempts atomic.Int32
	e.Register("boom", func(context.Context, Job) error {
		attempts.Add(1)
		return errTestBoom
	})
	id, _ := e.Submit(ctx, "boom", nil,
		At(clk.Now().Add(5*time.Second)), Retries(3), RetryBackoff(10*time.Second))

	e.step(ctx)
	e.pump(clk.Now().Add(5 * time.Second))
	waitAttempts(t, e, id, 1)

	rs, _ := e.store.(*sqliteStore).receipts(ctx, id)
	// first attempt: fired(attempt=1) + retry(attempt=1)
	var firedSeen, retrySeen bool
	for _, r := range rs {
		switch r.Kind {
		case "fired":
			firedSeen = r.Attempt == 1
		case "retry":
			retrySeen = r.Attempt == 1
		}
	}
	if !firedSeen || !retrySeen {
		t.Fatalf("receipts after attempt 1: %+v (want fired@1 + retry@1)", rs)
	}
}

func waitAttempts(t *testing.T, e *engine, id string, want int32) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		j, err := e.Get(context.Background(), id)
		// attempt N complete => row is waiting (backoff) or dead or completed
		if err == nil && j != nil && attemptCount(t, e, id) >= int(want) && j.State != Fired {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("never reached %d attempts", want)
}

func attemptCount(t *testing.T, e *engine, id string) int {
	t.Helper()
	rs, err := e.store.(*sqliteStore).receipts(context.Background(), id)
	if err != nil {
		return 0
	}
	var n int
	for _, r := range rs {
		if r.Kind == "fired" {
			n++
		}
	}
	return n
}
