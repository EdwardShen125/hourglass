package hourglass

import (
	"context"
	"testing"
	"time"
)

// newRetentionEngine: test engine with a 1h TTL.
func newRetentionEngine(t *testing.T) (*engine, *fakeClock) {
	t.Helper()
	s := newTestStore(t)
	clk := &fakeClock{now: time.Date(2026, 11, 1, 9, 0, 0, 0, time.UTC)}
	e := newEngine(s, clk, engineConfig{lookahead: 10 * time.Minute, admitLimit: 100, retention: time.Hour})
	t.Cleanup(func() { e.Close() })
	return e, clk
}

func TestRetentionPurgesTerminalRows(t *testing.T) {
	e, clk := newRetentionEngine(t)
	ctx := context.Background()
	e.Register("job", func(context.Context, Job) error { return nil })

	// three terminal jobs at t0
	ids := make([]string, 3)
	for i := range ids {
		id, _ := e.Submit(ctx, "job", nil, At(clk.Now().Add(time.Duration(i)*time.Second+time.Second)))
		ids[i] = id
	}
	e.step(ctx)
	e.pump(clk.Now().Add(5 * time.Second))
	for _, id := range ids {
		waitJobState(t, e, id, Completed)
	}

	// retention sweep with TTL 1h: nothing purged yet (rows are young)
	n, err := e.sweep(ctx, clk.Now())
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("purged %d young rows, want 0", n)
	}

	// age them past TTL, sweep again: jobs + their receipts go
	clk.Advance(2 * time.Hour)
	n, err = e.sweep(ctx, clk.Now())
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("purged %d rows, want 3", n)
	}
	for _, id := range ids {
		if _, err := e.Get(ctx, id); err == nil {
			t.Errorf("job %s still readable after purge", id)
		}
		rs, _ := e.store.(*sqliteStore).receipts(ctx, id)
		if len(rs) != 0 {
			t.Errorf("job %s receipts survived purge: %d", id, len(rs))
		}
	}
}

func TestRetentionKeepsLiveRows(t *testing.T) {
	e, clk := newRetentionEngine(t)
	ctx := context.Background()
	e.Register("job", func(context.Context, Job) error { return nil })

	// a waiting job far in the future + a completed one
	liveID, _ := e.Submit(ctx, "job", nil, At(clk.Now().Add(2*time.Hour)))
	doneID, _ := e.Submit(ctx, "job", nil, At(clk.Now().Add(time.Second)))
	e.step(ctx)
	e.pump(clk.Now().Add(5 * time.Second))
	waitJobState(t, e, doneID, Completed)

	clk.Advance(2 * time.Hour) // both past TTL by wall time
	n, err := e.sweep(ctx, clk.Now())
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("purged %d, want 1 (only the terminal row)", n)
	}
	if _, err := e.Get(ctx, liveID); err != nil {
		t.Errorf("waiting job purged: %v", err)
	}
}

func TestRetentionSweepIsBounded(t *testing.T) {
	// one sweep deletes at most maxSweep rows: bounded batches, never a
	// giant DELETE.
	e, clk := newRetentionEngine(t)
	ctx := context.Background()
	e.Register("job", func(context.Context, Job) error { return nil })
	for i := 0; i < 30; i++ {
		e.Submit(ctx, "job", nil, At(clk.Now().Add(time.Duration(i)*time.Second+time.Second)))
	}
	e.step(ctx)
	e.pump(clk.Now().Add(2 * time.Minute))
	time.Sleep(300 * time.Millisecond) // let the async flush write terminal_at
	clk.Advance(2 * time.Hour)

	n, err := e.store.(*sqliteStore).purgeTerminal(ctx, clk.Now(), time.Hour, 5)
	if err != nil {
		t.Fatal(err)
	}
	if n != 5 {
		t.Fatalf("bounded sweep deleted %d rows, want exactly the limit 5", n)
	}
}
