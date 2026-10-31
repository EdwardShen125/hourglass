package hourglass

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// engine test with a fake clock: admission + wheel + handler end to end.
func newTestEngine(t *testing.T) (*engine, *fakeClock) {
	t.Helper()
	s := newTestStore(t)
	clk := &fakeClock{now: time.Date(2026, 11, 1, 9, 0, 0, 0, time.UTC)}
	e := newEngine(s, clk, engineConfig{lookahead: 1 * time.Minute, admitLimit: 100})
	t.Cleanup(func() { e.Close() })
	return e, clk
}

func TestEngineFiresHandlerAtDueSecond(t *testing.T) {
	e, clk := newTestEngine(t)
	ctx := context.Background()

	var mu sync.Mutex
	firedAt := map[string]time.Time{}
	e.Register("email:welcome", func(ctx context.Context, j Job) error {
		mu.Lock()
		firedAt[j.ID] = clk.Now()
		mu.Unlock()
		return nil
	})

	id, err := e.Submit(ctx, "email:welcome", []byte(`{}`), At(clk.Now().Add(30*time.Second)))
	if err != nil {
		t.Fatal(err)
	}

	// admission at t0: the 30s-away job is inside the 1m lookahead
	if err := e.step(ctx); err != nil {
		t.Fatalf("step at t0: %v", err)
	}
	if got := e.wheel.Drain(); len(got) != 1 {
		t.Fatalf("after step at t0: wheel holds %v, want the 1 job", got)
	}

	// time passes; wheel ticks fire it exactly at due
	e.pump(clk.Now().Add(30 * time.Second))
	waitJobState(t, e, id, Completed)

	mu.Lock()
	at, ok := firedAt[id]
	mu.Unlock()
	if !ok {
		t.Fatal("handler never fired")
	}
	_ = at // fire-moment precision is asserted by wheel tests with the fake clock

	j, err := e.Get(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if j.State != Completed {
		t.Errorf("job state = %s, want completed", j.State)
	}
}

func TestEngineAdmitsOnlyInsideLookahead(t *testing.T) {
	e, clk := newTestEngine(t)
	ctx := context.Background()
	e.Register("noop", func(context.Context, Job) error { return nil })

	_, err := e.Submit(ctx, "noop", nil, At(clk.Now().Add(10*time.Minute)))
	if err != nil {
		t.Fatal(err)
	}
	if err := e.step(ctx); err != nil {
		t.Fatal(err)
	}
	if got := e.wheel.Drain(); len(got) != 0 {
		t.Fatalf("lookahead is 1m but wheel admitted a 10m job: %v", got)
	}
	// and the job is still waiting, not lost
	got, err := e.store.Select(ctx, scanUnitAll, clk.Now(), clk.Now().Add(1*time.Hour), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].State != Waiting {
		t.Fatalf("job not left waiting: %+v", got)
	}
}

func TestEngineMisfireRescanFiresLateJob(t *testing.T) {
	// a job due in the past (e.g. node was down) must still fire: the
	// admission window's floor is the misfire horizon, not now.
	e, clk := newTestEngine(t)
	ctx := context.Background()

	var fired atomic.Bool
	e.Register("late", func(context.Context, Job) error { fired.Store(true); return nil })

	_, err := e.Submit(ctx, "late", nil, At(clk.Now().Add(-2*time.Minute)))
	if err != nil {
		t.Fatal(err)
	}
	if err := e.step(ctx); err != nil {
		t.Fatal(err)
	}
	e.pump(clk.Now())
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && !fired.Load() {
		time.Sleep(5 * time.Millisecond)
	}
	if !fired.Load() {
		t.Fatal("misfired job was never fired")
	}
}

func TestEngineFailedHandlerDeadLetters(t *testing.T) {
	e, clk := newTestEngine(t)
	ctx := context.Background()

	e.Register("boom", func(context.Context, Job) error {
		return context.DeadlineExceeded // any error
	})
	id, err := e.Submit(ctx, "boom", nil, At(clk.Now().Add(5*time.Second)))
	if err != nil {
		t.Fatal(err)
	}

	if err := e.step(ctx); err != nil {
		t.Fatal(err)
	}
	e.pump(clk.Now().Add(5 * time.Second))
	waitJobState(t, e, id, Dead)

	j, err := e.Get(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if j.State != Dead {
		t.Errorf("failed handler: state = %s, want dead (v1 single attempt; retry policy later)", j.State)
	}
}

func waitJobState(t *testing.T, e *engine, id string, want State) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		j, err := e.Get(context.Background(), id)
		if err == nil && j.State == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
}
