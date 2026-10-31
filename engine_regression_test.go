package hourglass

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Review round-1 regression tests: engine concurrency defects.

// Watermark must not hide a fresh submit: Submit after a quiet-period
// watermark is set must wake admission.
func TestSubmitWakesQuiescentAdmission(t *testing.T) {
	s := newTestStore(t)
	clk := &fakeClock{now: time.Date(2026, 11, 1, 9, 0, 0, 0, time.UTC)}
	e := newEngine(s, clk, engineConfig{lookahead: 1 * time.Minute, admitLimit: 100})
	t.Cleanup(func() { e.Close() })
	ctx := context.Background()

	// far-future job sets the watermark
	e.Submit(ctx, "far", nil, At(clk.Now().Add(6*time.Hour)))
	if err := e.step(ctx); err != nil {
		t.Fatal(err)
	}
	if q := e.quietUntilNow(); q.Before(clk.Now()) {
		t.Fatalf("watermark not set: %v", q)
	}

	// near-term job submitted during the quiet window
	id, err := e.Submit(ctx, "near", nil, At(clk.Now().Add(5*time.Second)))
	if err != nil {
		t.Fatal(err)
	}
	if e.quietUntilNow().After(clk.Now()) {
		t.Fatal("Submit did not clear the quiescence watermark")
	}
	e.step(ctx)
	if got := e.wheel.Drain(); len(got) != 1 {
		t.Fatalf("near job not admitted after submit-wake: %v", got)
	}
	e.pump(clk.Now().Add(5 * time.Second))
	waitJobState(t, e, id, Completed)
}

// Watermark cap: even with a far-future MIN(due), the wake-up is capped
// at misfireHalo/2, so a due that crosses the lookahead while admission
// sleeps is still inside the scan floor when we wake.
func TestWatermarkCappedByMisfireHalo(t *testing.T) {
	s := newTestStore(t)
	clk := &fakeClock{now: time.Date(2026, 11, 1, 9, 0, 0, 0, time.UTC)}
	e := newEngine(s, clk, engineConfig{lookahead: 1 * time.Minute, admitLimit: 100})
	t.Cleanup(func() { e.Close() })
	ctx := context.Background()

	e.Submit(ctx, "far", nil, At(clk.Now().Add(6*time.Hour)))
	e.step(ctx)
	cap := clk.Now().Add(5 * time.Minute) // misfireHalo 10m / 2
	if q := e.quietUntilNow(); q.After(cap) {
		t.Fatalf("watermark %v beyond cap %v", q, cap)
	}
}

// A panicking handler must not wedge the batch or kill the worker.
func TestPanicHandlerIsContained(t *testing.T) {
	e, clk := newTestEngine(t)
	ctx := context.Background()
	var okRuns atomic.Int32
	e.Register("boom", func(context.Context, Job) error {
		panic("handler exploded")
	})
	e.Register("fine", func(context.Context, Job) error {
		okRuns.Add(1)
		return nil
	})

	var ids []string
	for i := 0; i < 4; i++ {
		id, _ := e.Submit(ctx, "boom", nil, At(clk.Now().Add(time.Duration(i+1)*time.Second)))
		ids = append(ids, id)
	}
	idFine, _ := e.Submit(ctx, "fine", nil, At(clk.Now().Add(5*time.Second)))
	ids = append(ids, idFine)

	e.step(ctx)
	e.pump(clk.Now().Add(10 * time.Second))

	// boom jobs dead-letter (panic -> error), fine completes, workers alive
	for _, id := range ids {
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			j, err := e.Get(ctx, id)
			if err == nil && (j.State == Dead || j.State == Completed) {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		j, _ := e.Get(ctx, id)
		if j == nil || (j.State != Dead && j.State != Completed) {
			t.Fatalf("job %s stuck in %v (batch wedged by panic?)", id, j)
		}
	}
	if okRuns.Load() != 1 {
		t.Fatalf("fine job ran %d times, want 1", okRuns.Load())
	}
	// workers still alive: another job still fires after the panics
	id2, _ := e.Submit(ctx, "fine", nil, At(clk.Now().Add(1*time.Second)))
	e.step(ctx)
	e.pump(clk.Now().Add(2 * time.Second))
	waitJobState(t, e, id2, Completed)
}

// Dispatch overflow must release back to waiting (anti-loss), not block
// the pump or lose the job.
func TestDispatchOverflowReleasesToWaiting(t *testing.T) {
	s := newTestStore(t)
	clk := &fakeClock{now: time.Date(2026, 11, 1, 9, 0, 0, 0, time.UTC)}
	// dispatchSize 2 with 1 worker: overflow path triggers deterministically
	e := newEngine(s, clk, engineConfig{lookahead: 10 * time.Minute, admitLimit: 100, workers: 1, dispatchSize: 2})
	t.Cleanup(func() { e.Close() })
	ctx := context.Background()

	release := make(chan struct{})
	e.Register("slow", func(context.Context, Job) error {
		<-release
		return nil
	})
	n := 50
	for i := 0; i < n; i++ {
		e.Submit(ctx, "slow", nil, At(clk.Now().Add(time.Duration(i+1)*time.Second)))
	}
	e.step(ctx)
	// pump with workers parked: dispatch fills, the rest must spill to
	// waiting without blocking
	done := make(chan struct{})
	go func() { e.pump(clk.Now().Add(time.Minute)); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("pump blocked on a full dispatch channel")
	}

	// spilled rows are waiting again (admissible once workers drain):
	// 50 fired, 1 parked in the worker, 2 buffered -> >= 47 must be
	// back in waiting
	if w := countState(t, e, "waiting"); w < 47 {
		t.Fatalf("only %d/47 overflowed jobs released to waiting", w)
	}
	close(release) // let parked workers finish
}

func countState(t *testing.T, e *engine, state string) int {
	t.Helper()
	js, err := e.store.(*sqliteStore).listAll(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, j := range js {
		if string(j.State) == state {
			n++
		}
	}
	return n
}

// Close must not panic when racing a live pump, and Handle.Close must
// stop the workers (no goroutine leak).
func TestCloseUnderLoadIsSafe(t *testing.T) {
	e, clk := newTestEngine(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			e.Submit(ctx, "job", nil, At(clk.Now().Add(time.Duration(i)*time.Millisecond)))
		}(i)
	}
	e.Register("job", func(context.Context, Job) error { return nil })
	wg.Wait()
	e.step(ctx)
	go e.pump(clk.Now().Add(time.Minute))
	time.Sleep(10 * time.Millisecond)
	if err := e.Close(); err != nil {
		t.Fatalf("Close error: %v", err)
	}
}
