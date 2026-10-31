package hourglass

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestFireWritesReceipt(t *testing.T) {
	e, clk := newTestEngine(t)
	ctx := context.Background()
	e.Register("job", func(context.Context, Job) error { return nil })

	id, _ := e.Submit(ctx, "job", []byte(`{}`), At(clk.Now().Add(5*time.Second)))
	e.step(ctx)
	e.pump(clk.Now().Add(5 * time.Second))
	waitJobState(t, e, id, Completed)

	// receipt: one fired, one completed transition, timestamps, attempt=1
	rs, err := e.store.(*sqliteStore).receipts(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(rs) != 2 {
		t.Fatalf("receipts = %d, want 2 (fired, completed)", len(rs))
	}
	if rs[0].Kind != "fired" || rs[1].Kind != "completed" {
		t.Fatalf("kinds = %s,%s want fired,completed", rs[0].Kind, rs[1].Kind)
	}
	if rs[0].Attempt != 1 {
		t.Errorf("attempt = %d, want 1", rs[0].Attempt)
	}
}

func TestDeadJobHasErrorReceipt(t *testing.T) {
	e, clk := newTestEngine(t)
	ctx := context.Background()
	e.Register("boom", func(context.Context, Job) error { return errTestBoom })
	id, _ := e.Submit(ctx, "boom", nil, At(clk.Now().Add(5*time.Second)))

	e.step(ctx)
	e.pump(clk.Now().Add(5 * time.Second))
	waitJobState(t, e, id, Dead)

	rs, _ := e.store.(*sqliteStore).receipts(ctx, id)
	if len(rs) != 2 || rs[1].Kind != "dead" {
		t.Fatalf("receipts = %+v, want [fired dead]", rs)
	}
	if rs[1].Error == "" {
		t.Error("dead receipt missing error text")
	}
}

func TestCancelWritesReceiptRow(t *testing.T) {
	e, clk := newTestEngine(t)
	ctx := context.Background()
	id, _ := e.Submit(ctx, "job", nil, At(clk.Now().Add(5*time.Second)))
	if err := e.Cancel(ctx, id, "user asked"); err != nil {
		t.Fatal(err)
	}
	rs, _ := e.store.(*sqliteStore).receipts(ctx, id)
	if len(rs) != 1 || rs[0].Kind != "canceled" {
		t.Fatalf("receipts = %+v, want one canceled", rs)
	}
	if rs[0].Reason != "user asked" {
		t.Errorf("reason = %q", rs[0].Reason)
	}
}

func TestCloseDrainsInFlightDispatch(t *testing.T) {
	// a job dispatched but not yet handled must still complete when
	// Close runs: fire what is due, wait for handlers.
	e, clk := newTestEngine(t)
	ctx := context.Background()
	started := make(chan struct{})
	release := make(chan struct{})
	var handled atomic.Bool
	e.Register("slow", func(context.Context, Job) error {
		close(started)
		<-release
		handled.Store(true)
		return nil
	})
	id, _ := e.Submit(ctx, "slow", nil, At(clk.Now().Add(1*time.Second)))
	e.step(ctx)
	e.pump(clk.Now().Add(1 * time.Second))
	<-started // handler is parked inside fireEntry

	go func() { time.Sleep(100 * time.Millisecond); close(release) }()
	if err := e.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !handled.Load() {
		t.Fatal("Close returned before in-flight handler finished")
	}
	j, _ := e.Get(ctx, id)
	if j.State != Completed {
		t.Errorf("state after Close = %s, want completed", j.State)
	}
}

func TestJitterWindowDrawnOnceAtSubmit(t *testing.T) {
	// due = scheduled + rand(0, jitter); drawn once, stored, visible.
	e, clk := newTestEngine(t)
	ctx := context.Background()
	sched := clk.Now().Add(10 * time.Minute)

	id, err := e.Submit(ctx, "job", nil, At(sched), Jitter(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	j, _ := e.Get(ctx, id)
	if j.Due.Before(sched) || j.Due.After(sched.Add(2*time.Minute)) {
		t.Fatalf("jittered due %v outside [%v, %v+2m]", j.Due, sched, sched)
	}
	if j.ScheduledAt.IsZero() || !j.ScheduledAt.Equal(sched) {
		t.Errorf("scheduled_at = %v, want the pre-jitter time %v", j.ScheduledAt, sched)
	}

	// re-read: the stored due must not change (drawn once, durable)
	j2, _ := e.Get(ctx, id)
	if !j2.Due.Equal(j.Due) {
		t.Fatalf("due drifted across reads: %v -> %v", j.Due, j2.Due)
	}
}

func TestJitterDeterministicUnderSeed(t *testing.T) {
	// same seed -> same draw: proves the draw is ours, not wall-clock
	s := newTestStore(t)
	d1 := drawJitter(2*time.Minute, 1)
	d2 := drawJitter(2*time.Minute, 1)
	if d1 != d2 {
		t.Fatalf("seeded draws differ: %v vs %v", d1, d2)
	}
	if d1 < 0 || d1 > 2*time.Minute {
		t.Fatalf("draw out of window: %v", d1)
	}
	_ = s
}
