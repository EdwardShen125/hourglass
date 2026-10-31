package hourglass

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"testing"
	"time"
)

func TestTransitionIsConditional(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	base := time.Date(2026, 11, 1, 9, 0, 0, 0, time.UTC)
	id := mustSubmit(t, s, "a", base)

	// waiting -> fired directly must fail: only acquired -> fired is legal
	if err := s.transition(ctx, id, Waiting, Fired); err == nil {
		t.Fatal("waiting->fired accepted, want rejection (illegal edge)")
	}
	j, _ := s.Get(ctx, id)
	if j.State != Waiting {
		t.Fatalf("illegal transition mutated state to %s", j.State)
	}

	// legal path: waiting -> acquired -> fired
	if _, err := s.Acquire(ctx, scanUnitAll, 1, []string{id}); err != nil {
		t.Fatal(err)
	}
	if err := s.transition(ctx, id, Acquired, Fired); err != nil {
		t.Fatalf("acquired->fired: %v", err)
	}
	if err := s.transition(ctx, id, Fired, Completed); err != nil {
		t.Fatalf("fired->completed: %v", err)
	}
	// fired -> completed again must fail: row is already completed
	if err := s.transition(ctx, id, Fired, Completed); err == nil {
		t.Fatal("double complete accepted")
	}
}

func TestFirePathRunsHandlerOutsideWheelLock(t *testing.T) {
	// a slow handler must not block wheel Offer/Cancel: the fire path
	// hands entries to a worker pool instead of running under w.mu.
	e, clk := newTestEngine(t)
	ctx := context.Background()

	release := make(chan struct{})
	e.Register("slow", func(context.Context, Job) error {
		<-release
		return nil
	})
	fast := make(chan string, 1)
	e.Register("fast", func(_ context.Context, j Job) error {
		fast <- j.ID
		return nil
	})

	_, err := e.Submit(ctx, "slow", nil, At(clk.Now().Add(5*time.Second)))
	if err != nil {
		t.Fatal(err)
	}
	idFast, _ := e.Submit(ctx, "fast", nil, At(clk.Now().Add(6*time.Second)))

	e.step(ctx)
	e.pump(clk.Now().Add(6 * time.Second)) // both fire; slow handler parks

	// fast must complete even while slow is parked (worker pool > 1)
	select {
	case <-fast:
	case <-time.After(2 * time.Second):
		t.Fatal("fast handler starved behind a parked slow handler — fire path is serialized")
	}

	// wheel must still accept new offers while a handler runs
	done := make(chan struct{})
	go func() { e.pump(clk.Now().Add(7 * time.Second)); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("pump blocked while handler parked — fire runs under the wheel lock")
	}

	close(release)
	waitJobState(t, e, idFast, Completed)
}

func TestNewIDRandomAndUnique(t *testing.T) {
	seen := make(map[string]struct{})
	for i := 0; i < 10000; i++ {
		id := newID()
		if len(id) != 32 { // 16 bytes hex
			t.Fatalf("id %q: len = %d, want 32", id, len(id))
		}
		if _, err := hex.DecodeString(id); err != nil {
			t.Fatalf("id %q not hex: %v", id, err)
		}
		if _, dup := seen[id]; dup {
			t.Fatalf("duplicate id %s at iteration %d", id, i)
		}
		seen[id] = struct{}{}
	}
}

var _ = rand.Reader
