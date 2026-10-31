package hourglass

import (
	"context"
	"testing"
	"time"
)

func TestCancelBeforeAdmission(t *testing.T) {
	e, clk := newTestEngine(t)
	ctx := context.Background()
	e.Register("noop", func(context.Context, Job) error { return nil })

	id, _ := e.Submit(ctx, "noop", nil, At(clk.Now().Add(30*time.Second)))
	if err := e.Cancel(ctx, id, "user asked"); err != nil {
		t.Fatalf("Cancel: %v", err)
	}

	j, _ := e.Get(ctx, id)
	if j.State != Canceled {
		t.Fatalf("state = %s, want canceled", j.State)
	}
	if j.CancelReason != "user asked" {
		t.Errorf("cancel reason = %q, want persisted", j.CancelReason)
	}

	// canceled rows must never be admitted afterwards
	if err := e.step(ctx); err != nil {
		t.Fatal(err)
	}
	if got := e.wheel.Drain(); len(got) != 0 {
		t.Fatalf("canceled job admitted: %v", got)
	}
}

func TestCancelAfterAdmissionBeforeFire(t *testing.T) {
	e, clk := newTestEngine(t)
	ctx := context.Background()

	var fired int
	e.Register("job", func(context.Context, Job) error { fired++; return nil })
	id, _ := e.Submit(ctx, "job", nil, At(clk.Now().Add(30*time.Second)))

	if err := e.step(ctx); err != nil {
		t.Fatal(err)
	} // now in wheel

	if err := e.Cancel(ctx, id, "changed mind"); err != nil {
		t.Fatalf("Cancel after admission: %v", err)
	}

	e.pump(clk.Now().Add(30 * time.Second))
	time.Sleep(100 * time.Millisecond) // let workers drain the dispatch queue
	if fired != 0 {
		t.Fatalf("canceled-in-wheel job fired %d times", fired)
	}
	j, _ := e.Get(ctx, id)
	if j.State != Canceled {
		t.Errorf("state = %s, want canceled (receipt)", j.State)
	}
}

func TestCancelAlreadyFiredIsRejected(t *testing.T) {
	e, clk := newTestEngine(t)
	ctx := context.Background()
	e.Register("job", func(context.Context, Job) error { return nil })
	id, _ := e.Submit(ctx, "job", nil, At(clk.Now().Add(5*time.Second)))

	e.step(ctx)
	e.pump(clk.Now().Add(5 * time.Second))
	waitJobState(t, e, id, Completed)

	if err := e.Cancel(ctx, id, "too late"); err == nil {
		t.Fatal("Cancel on completed job returned nil, want error")
	}
	// state untouched
	j, _ := e.Get(ctx, id)
	if j.State != Completed {
		t.Errorf("state = %s, want still completed", j.State)
	}
}
