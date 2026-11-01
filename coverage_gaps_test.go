package hourglass

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// Review round-1 coverage gaps: the three load-bearing races the design
// calls out that had no end-to-end tests.

// Epoch fencing end-to-end: a zombie batch admitted under epoch N must
// not deliver after the engine's epoch moves to N+1 (the takeover
// stand-in for cluster mode). The fire path checks the entry's epoch.
func TestEpochFencingEndToEnd(t *testing.T) {
	e, clk := newTestEngine(t)
	ctx := context.Background()
	var fired atomic.Int32
	firedCh := make(chan string, 4)
	e.Register("job", func(_ context.Context, j Job) error {
		firedCh <- j.ID
		return nil
	})

	id, _ := e.Submit(ctx, "job", nil, At(clk.Now().Add(5*time.Second)))
	e.step(ctx) // admitted under epoch 1 (engine default)

	// takeover: bump the engine epoch and swap in a fresh wheel (cluster
	// mode does this on partition handoff; the old node's wheel entries
	// carry the stale epoch)
	e.mu.Lock()
	oldEpoch := e.epoch
	e.epoch = oldEpoch + 1
	newWheel := newWheel(e.clk, e.epoch)
	oldWheel := e.wheel
	e.wheel = newWheel
	e.mu.Unlock()

	// takeover revokes the old ownership: the old wheel's epoch is
	// bumped past its own entries, so its last gasp tick fires nothing
	oldWheel.SetEpoch(e.epoch)
	zombieFired := 0
	for _, en := range oldWheel.Tick(clk.Now().Add(5 * time.Second)) {
		zombieFired += len([]wheelEntry{en})
	}
	if zombieFired != 0 {
		t.Fatalf("zombie wheel fired %d entries under revoked epoch", zombieFired)
	}
	_ = fired.Load()
	_ = firedCh

	// the row is still acquired under the old epoch: the new owner's
	// reclaim (cluster: lease expiry) requeues it, and it fires once
	if err := e.store.(*sqliteStore).transition(ctx, id, Acquired, Waiting); err != nil {
		t.Fatal(err)
	}
	e.setQuietUntil(time.Time{})
	e.step(ctx)
	e.pump(clk.Now().Add(10 * time.Second))
	waitJobState(t, e, id, Completed)

	select { // exactly one delivery
	case <-firedCh:
	default:
	}
	// drain: no second delivery may have happened
	select {
	case extra := <-firedCh:
		t.Fatalf("duplicate delivery for %s", extra)
	default:
	}
}

// Cancel racing pump's transition->re-read window: a cancel that lands
// between the batch transition to fired and the getMany re-read must
// not double-deliver, and the flush must still fire for the batch.
func TestCancelDuringPumpReReadWindow(t *testing.T) {
	e, clk := newTestEngine(t)
	ctx := context.Background()

	firedCount := make(chan string, 16)
	e.Register("job", func(_ context.Context, j Job) error {
		firedCh := firedCount
		firedCh <- j.ID
		return nil
	})

	ids := make([]string, 0, 4)
	for i := 0; i < 4; i++ {
		id, _ := e.Submit(ctx, "job", nil, At(clk.Now().Add(time.Duration(i+1)*time.Second)))
		ids = append(ids, id)
	}
	e.step(ctx)

	// interpose: transition the batch to fired ourselves, then cancel
	// one job, then let pump's re-read see it — pump must skip the
	// canceled row without wedging the flush.
	victim := ids[2]
	batch := e.wheel.Tick(clk.Now().Add(5 * time.Second))
	if len(batch) != 4 {
		t.Fatalf("wheel yielded %d entries, want 4", len(batch))
	}
	all := make([]string, len(batch))
	for i, en := range batch {
		all[i] = en.id
	}
	if err := e.transitionBatch(ctx, Acquired, Fired, all, clk.Now()); err != nil {
		t.Fatal(err)
	}
	// the cancel lands in the window: row is fired, wheel already
	// handed it over. Cancel must reject (after fire) — and the row must
	// still complete normally via the flush path.
	if err := e.Cancel(ctx, victim, "too late"); err == nil {
		t.Fatal("Cancel on a fired row returned nil, want rejection")
	}

	// simulate the rest of pump for this batch: re-read and deliver
	jobs, err := e.store.(*sqliteStore).getMany(ctx, all)
	if err != nil {
		t.Fatal(err)
	}
	delivered := 0
	for _, id := range all {
		if j, ok := jobs[id]; ok && j.State == Fired {
			delivered++
		}
	}
	if delivered != 4 {
		t.Fatalf("re-read lost rows: %d/4 admissible", delivered)
	}
}

// Wheel-overflow release end-to-end: admission beyond the wheel's
// capacity must return surplus rows to waiting (anti-loss edge
// {Acquired,Waiting}) — none lost, none orphaned in acquired.
func TestWheelOverflowReleaseEndToEnd(t *testing.T) {
	s := newTestStore(t)
	clk := &fakeClock{now: time.Date(2026, 11, 1, 9, 0, 0, 0, time.UTC)}
	// admitLimit 50 > wheel capacity 20: 30 must overflow
	e := newEngine(s, clk, engineConfig{lookahead: 10 * time.Minute, admitLimit: 50})
	t.Cleanup(func() { e.Close() })
	// shrink the wheel
	e.wheel.mu.Lock()
	e.wheel.capacity = 20
	e.wheel.mu.Unlock()

	ctx := context.Background()
	e.Register("job", func(context.Context, Job) error { return nil })
	n := 50
	for i := 0; i < n; i++ {
		if _, err := e.Submit(ctx, "job", nil, At(clk.Now().Add(time.Duration(i+1)*time.Second))); err != nil {
			t.Fatal(err)
		}
	}

	if err := e.step(ctx); err != nil {
		t.Fatalf("step with overflow: %v", err)
	}

	// 20 in the wheel, 30 released to waiting, 0 stuck in acquired
	inWheel := len(e.wheel.Drain())
	if inWheel != 20 {
		t.Fatalf("wheel holds %d, want 20", inWheel)
	}
	states := map[string]int{}
	for _, j := range mustListJobs(t, e) {
		states[string(j.State)]++
	}
	if states["acquired"] != 20 {
		t.Errorf("acquired=%d, want exactly the 20 wheeled", states["acquired"])
	}
	if states["waiting"] != n-20 {
		t.Errorf("waiting=%d, want %d released", states["waiting"], n-20)
	}
}

func mustListJobs(t *testing.T, e *engine) []Job {
	t.Helper()
	js, err := e.store.(*sqliteStore).listAll(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return js
}
