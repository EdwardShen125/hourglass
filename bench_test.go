package hourglass

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"sync"
	"testing"
	"time"
)

// BenchFirePrecision: |actual − scheduled| distribution for hourglass
// (wheel) vs a poll-based baseline, same randomized schedule.
//
//	Fake clock; zero handler cost; the delta measures only the
//	scheduling mechanism's latency.
//
// Fairness notes (review): hourglass is measured through its full
// async pipeline (admission -> wheel -> dispatch -> flush) while the
// poller baseline is an idealized synchronous loop (no dispatch layer,
// no per-fire writes) — the bias is AGAINST hourglass. Intervals:
// hourglass tick 1s vs poller 5s is a default-vs-default comparison;
// both sides also count their mechanism's reads. The gates assert
// hourglass tail dominance, not absolute equality.

func TestBenchmarkFirePrecision(t *testing.T) {
	if testing.Short() {
		t.Skip("benchmark")
	}
	const (
		nJobs  = 5000
		span   = 10 * time.Minute
		lookIn = 1 * time.Minute // hourglass lookahead
		tickIn = 1 * time.Second // hourglass tick
		pollEv = 5 * time.Second // poller interval (asynq default-ish)
	)

	rng := rand.New(rand.NewSource(42))
	base := time.Date(2026, 11, 1, 9, 0, 0, 0, time.UTC)
	type sched struct {
		id  string
		at  time.Time
		got time.Time
	}
	plan := make([]sched, nJobs)
	for i := range plan {
		off := time.Duration(rng.Int63n(int64(span)))
		plan[i] = sched{id: fmt.Sprintf("j%d", i), at: base.Add(off.Truncate(time.Second))}
	}

	// ---- hourglass: admit in windows, wheel fires at the second ----
	sHG := newTestStore(t)
	clk := &fakeClock{now: base}
	eHG := newEngine(sHG, clk, engineConfig{lookahead: lookIn, admitLimit: nJobs})
	for i := range plan {
		eHG.store.Insert(context.Background(), &Job{ID: plan[i].id, Topic: "t", Due: plan[i].at, State: Waiting})
	}
	var mu sync.Mutex
	gotHG := map[string]time.Time{}
	eHG.Register("t", func(_ context.Context, j Job) error {
		mu.Lock()
		gotHG[j.ID] = clk.Now()
		mu.Unlock()
		return nil
	})
	// drive: 1s ticks over the span
	for now := base; !now.After(base.Add(span).Add(2 * tickIn)); now = now.Add(tickIn) {
		eHG.pump(now)
		eHG.step(context.Background())
		clk.Advance(tickIn)
	}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(gotHG)
		mu.Unlock()
		if n == nJobs {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	for _, p := range plan {
		if _, ok := gotHG[p.id]; !ok {
			t.Fatalf("hourglass lost job %s", p.id)
		}
	}
	// collect errors: wheel fire time = due tick time (second-truncated)
	errsHG := make([]float64, 0, nJobs)
	for _, p := range plan {
		g := gotHG[p.id]
		errsHG = append(errsHG, math.Abs(float64(g.Sub(p.at).Milliseconds())))
	}

	// ---- poller: query the store every pollEv, fire what's due ----
	sPL := newTestStore(t)
	for i := range plan {
		sPL.Insert(context.Background(), &Job{ID: plan[i].id, Topic: "t", Due: plan[i].at, State: Waiting})
	}
	errsPL := make([]float64, 0, nJobs)
	fired := make(map[string]bool)
	now := base
	pollDeadline := base.Add(span + 2*pollEv) // every job is due within span
	for len(errsPL) < nJobs {
		now = now.Add(pollEv)
		if now.After(pollDeadline) {
			t.Fatalf("poller baseline failed to fire all %d jobs by %v", nJobs, pollDeadline)
		}
		js, err := sPL.Select(context.Background(), scanUnitAll, base, now, nJobs)
		if err != nil {
			t.Fatal(err)
		}
		for _, j := range js {
			if fired[j.ID] {
				continue
			}
			fired[j.ID] = true
			errsPL = append(errsPL, math.Abs(float64(now.Sub(j.Due).Milliseconds())))
		}
	}

	pct := func(xs []float64, p float64) float64 {
		s := append([]float64{}, xs...)
		sort.Float64s(s)
		idx := int(float64(len(s)) * p)
		if idx >= len(s) {
			idx = len(s) - 1
		}
		return s[idx]
	}
	t.Logf("precision |actual-scheduled| ms over %d jobs / %v span:", nJobs, span)
	t.Logf("  hourglass(wheel,1s tick): p50=%v p99=%v p999=%v max=%v",
		pct(errsHG, .50), pct(errsHG, .99), pct(errsHG, .999), pct(errsHG, 1.0))
	t.Logf("  poller(5s interval):      p50=%v p99=%v p999=%v max=%v",
		pct(errsPL, .50), pct(errsPL, .99), pct(errsPL, .999), pct(errsPL, 1.0))

	// GATES (review: benchmarks were t.Logf-only demos). The wheel is
	// second-granular with a 1s tick: p99 must sit at ~1 tick and max at
	// ~2 ticks (tick truncation + one tick of dispatch). A regression to
	// poller-grade precision (seconds of drift) fails here.
	if p99 := pct(errsHG, .99); p99 > 1500 {
		t.Errorf("hourglass fire precision regressed: p99=%vms, gate 1500ms", p99)
	}
	if mx := pct(errsHG, 1.0); mx > 3000 {
		t.Errorf("hourglass fire precision regressed: max=%vms, gate 3000ms", mx)
	}
	// and hourglass must dominate the 5s poller at the tail
	if pct(errsHG, .99) >= pct(errsPL, .99) {
		t.Errorf("hourglass p99 (%vms) not better than 5s poller p99 (%vms)",
			pct(errsHG, .99), pct(errsPL, .99))
	}
}

// BenchQuiescence: database queries while idle with a large pending
// backlog. The wheel admits in windows (queries bounded by window
// math); a poller scans the backlog every interval forever.
func TestBenchmarkQuiescence(t *testing.T) {
	if testing.Short() {
		t.Skip("benchmark")
	}
	const (
		pending   = 100000
		idleFor   = 10 * time.Minute
		tickIn    = 1 * time.Second
		pollEv    = 5 * time.Second
		lookahead = 1 * time.Minute
	)

	base := time.Date(2026, 11, 1, 9, 0, 0, 0, time.UTC)
	s := newTestStore(t)
	ctx := context.Background()
	// all pending jobs due AFTER the idle window (quiet rows)
	for i := 0; i < pending; i++ {
		due := base.Add(24 * time.Hour)
		if err := s.Insert(ctx, &Job{ID: fmt.Sprintf("q%d", i), Topic: "t", Due: due, State: Waiting}); err != nil {
			t.Fatal(err)
		}
	}

	// hourglass idle: 1s ticks for 10 minutes, everything far outside
	clk := &fakeClock{now: base}
	e := newEngine(s, clk, engineConfig{lookahead: lookahead, admitLimit: 1000})
	before := e.readCount()
	for now := base; !now.After(base.Add(idleFor)); now = now.Add(tickIn) {
		e.pump(now)
		e.step(ctx)
	}
	hgQueries := e.readCount() - before

	// poller idle: scans every 5s over the same backlog
	s2 := newTestStore(t)
	for i := 0; i < pending; i++ {
		s2.Insert(ctx, &Job{ID: fmt.Sprintf("q%d", i), Topic: "t", Due: base.Add(24 * time.Hour), State: Waiting})
	}
	before2 := s2.readCount()
	for now := base; !now.After(base.Add(idleFor)); now = now.Add(pollEv) {
		s2.Select(ctx, scanUnitAll, base, now.Add(pollEv), 1000)
	}
	plQueries := s2.readCount() - before2

	t.Logf("quiescence over %v idle, %d pending:", idleFor, pending)
	t.Logf("  hourglass: %d statements (%.1f/min)", hgQueries, float64(hgQueries)/(idleFor.Minutes()))
	t.Logf("  poller:    %d statements (%.1f/min)", plQueries, float64(plQueries)/(idleFor.Minutes()))

	// GATES: with the quiescence watermark active, admission does ~2
	// reads per quiet period (empty scan + MIN(due)) over 10 minutes —
	// not one per tick. Without it, hourglass reads every second (600+).
	if hgQueries > 10 {
		t.Errorf("hourglass quiescence regressed: %d reads over %v idle, gate 10 (watermark inactive?)", hgQueries, idleFor)
	}
	// the poller cannot help itself: one scan per interval forever
	if plQueries < int64(idleFor/pollEv)-1 {
		t.Errorf("poller baseline suspiciously quiet: %d reads, expected ~%d", plQueries, int64(idleFor/pollEv))
	}
}
