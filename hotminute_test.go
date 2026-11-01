package hourglass

import (
	"context"
	"testing"
	"time"
)

// BenchHotMinute (DESIGN.md benchmark 2): N jobs due at the same second.
// Measures: time to drain, statements issued (write pressure on the
// database), and that nothing is lost or duplicated.
//
// Fake clock: drain time measures the engine's work, not wall-clock
// scheduling noise. Statement counts measure DB pressure directly.
func TestBenchmarkHotMinute(t *testing.T) {
	if testing.Short() {
		t.Skip("benchmark")
	}
	// DESIGN: "N=100K jobs due at the same second". The race detector
	// slows the engine 10-20x; under -race we verify the same logic
	// shape at 1/10th scale (race is for logic, throughput runs without).
	const n = 100000
	nJobs := n
	if raceEnabled {
		nJobs = 10000
	}

	s := newTestStore(t)
	clk := &fakeClock{now: time.Date(2026, 11, 1, 9, 0, 0, 0, time.UTC)}
	// large wheel + dispatch + workers; small lookahead so admission is
	// one batch
	e := newEngine(s, clk, engineConfig{
		lookahead: 1 * time.Minute,
		// admitLimit below n forces multiple admission rounds — the
		// realistic shape (1000/tick default)
		admitLimit:   50000,
		workers:      16,
		dispatchSize: n,
	})
	t.Cleanup(func() { e.Close() })
	ctx := context.Background()

	var fired, dupes, lost int
	var mu = make(chan struct{}, 1)
	mu <- struct{}{}
	withMu := func(f func()) { <-mu; f(); mu <- struct{}{} }

	e.Register("storm", func(_ context.Context, j Job) error {
		withMu(func() { fired++ })
		return nil
	})

	// seed: n jobs all due at t0+60s (inside lookahead, one batch)
	storm := time.Date(2026, 11, 1, 9, 1, 0, 0, time.UTC)
	if err := s.seedStorm(ctx, nJobs, storm); err != nil {
		t.Fatalf("seed %d jobs: %v", nJobs, err)
	}

	// drive: ticks across the storm; the fake clock holds still while
	// the engine works, so 'drain time' is wall-clock work time
	start := time.Now()
	for now := clk.Now(); !now.After(storm.Add(2 * time.Minute)); now = now.Add(time.Second) {
		e.pump(now)
		if err := e.step(ctx); err != nil {
			t.Fatalf("step at %v: %v", now, err)
		}
		// settle async dispatch between ticks
		for i := 0; i < 50 && len(e.dispatch) > 0; i++ {
			time.Sleep(10 * time.Millisecond)
		}
	}
	writes := e.stmtCount()

	// wait for the flush to land everything
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		st := e.storeStats(ctx)
		done := false
		for _, line := range st.JobsByState {
			if string(line.State) == "completed" && int(line.Count) == nJobs {
				done = true
			}
		}
		if done {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	drain := time.Since(start)

	// correctness gates: nothing lost, nothing duplicated
	final := e.storeStats(ctx)
	var completed int64
	for _, line := range final.JobsByState {
		completed += line.Count
		switch string(line.State) {
		case "waiting", "acquired", "fired":
			t.Errorf("jobs stuck in %s after drain: %d", line.State, line.Count)
		}
	}
	if completed != int64(nJobs) {
		t.Errorf("completed=%d want %d (lost=%d)", completed, nJobs, nJobs-int(completed))
	}
	withMu(func() { lost = nJobs - fired })
	if fired != nJobs {
		t.Errorf("handler ran %d times, want exactly %d (lost=%d)", fired, nJobs, lost)
	}
	_ = dupes

	t.Logf("hot minute: %d jobs due the same second%s", nJobs, map[bool]string{true: " (-race: reduced from 100K)", false: ""}[raceEnabled])
	t.Logf("  drain wall-clock:      %v (fake clock; engine work only)", drain.Round(time.Millisecond))
	t.Logf("  write statements:      %d (%.2f per job; unbatched path is 4+/job)", writes, float64(writes)/float64(nJobs))
	t.Logf("  handler deliveries:    %d (no loss, no duplication)", fired)

	// GATE: batching must hold under the storm. Unbatched = 4+ writes
	// per job (>=400K); batched round-trips collapse this. Loose gate:
	// 1 write per job ceiling.
	if writes > int64(nJobs) {
		t.Errorf("write amplification regressed: %d writes for %d jobs (gate: <= n)", writes, nJobs)
	}
}
