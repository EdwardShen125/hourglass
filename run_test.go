package hourglass_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/EdwardShen125/hourglass"
)

// real-clock integration: the README 30-second start, verbatim shape.
func TestRunFiresRealClock(t *testing.T) {
	h := openTestDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var fired atomic.Int32
	h.Register("email:welcome", func(ctx context.Context, j hourglass.Job) error {
		fired.Add(1)
		return nil
	})

	if _, err := h.Submit(ctx, "email:welcome", []byte(`{}`), hourglass.Delay(1500*time.Millisecond)); err != nil {
		t.Fatal(err)
	}

	go h.Run(ctx)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && fired.Load() == 0 {
		time.Sleep(50 * time.Millisecond)
	}
	if fired.Load() != 1 {
		t.Fatalf("job did not fire within 5s (fired=%d)", fired.Load())
	}
}

func TestRunStopsOnContextCancel(t *testing.T) {
	h := openTestDB(t)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() { h.Run(ctx); close(done) }()

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after context cancel")
	}
}
