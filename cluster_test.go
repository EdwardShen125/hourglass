package hourglass

import (
	"context"
	"sync"
	"testing"
	"time"
)

// Step b of cluster mode: one Cluster drives N per-shard engines over
// one store. The shard is part of the job's identity from submit on.

func TestEngineBindsToAScanUnit(t *testing.T) {
	// an engine constructed for shard 17 only selects/acquires within
	// shard 17 — the scanUnitAll hardcode is gone
	s := newTestStore(t)
	ctx := context.Background()
	base := time.Date(2026, 11, 1, 9, 0, 0, 0, time.UTC)

	j17 := &Job{Topic: "t", Due: base, State: Waiting, Shard: 17}
	j18 := &Job{Topic: "t", Due: base, State: Waiting, Shard: 18}
	if err := s.Insert(ctx, j17); err != nil {
		t.Fatal(err)
	}
	if err := s.Insert(ctx, j18); err != nil {
		t.Fatal(err)
	}

	clk := &fakeClock{now: base}
	e17 := newEngineForShard(s, clk, engineConfig{lookahead: time.Minute, admitLimit: 10}, ScanUnit("17"), 1)
	defer e17.Close()

	if err := e17.step(ctx); err != nil {
		t.Fatal(err)
	}
	// only shard 17's job was admitted by e17
	if got := e17.wheel.Drain(); len(got) != 1 || got[0] != j17.ID {
		t.Fatalf("shard-17 engine admitted %v, want exactly [%s]", got, j17.ID)
	}
	// shard 18's job untouched: still waiting
	j, err := s.Get(ctx, j18.ID)
	if err != nil {
		t.Fatal(err)
	}
	if j.State != Waiting {
		t.Fatalf("shard-18 job leaked into shard-17 engine: %s", j.State)
	}
}

func TestClusterRunsShardsInParallel(t *testing.T) {
	// a Cluster owns a set of shards, each with its own engine; jobs in
	// different shards fire independently through the same pump cadence
	s := newTestStore(t)
	ctx := context.Background()
	base := time.Date(2026, 11, 1, 9, 0, 0, 0, time.UTC)
	clk := &fakeClock{now: base}

	shards := []ScanUnit{ScanUnit("3"), ScanUnit("7"), ScanUnit("9")}
	c := newCluster(s, clk, engineConfig{lookahead: time.Minute, admitLimit: 100}, shards)
	defer c.Close()

	fired := make(chan string, 64)
	c.Register("t", func(_ context.Context, j Job) error {
		fired <- j.ID
		return nil
	})

	// one job per shard, all due at base+5s
	ids := map[string]ScanUnit{}
	for _, sh := range shards {
		j := &Job{Topic: "t", Due: base.Add(5 * time.Second), State: Waiting, Shard: shardOf(sh)}
		if err := s.Insert(ctx, j); err != nil {
			t.Fatal(err)
		}
		ids[j.ID] = sh
	}

	// drive: each shard's engine steps and pumps on its own cadence
	c.stepAll(ctx)
	c.pumpAll(clk.Now().Add(10 * time.Second))

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && len(fired) < len(shards) {
		time.Sleep(10 * time.Millisecond)
	}
	if len(fired) != len(shards) {
		t.Fatalf("fired %d, want %d (one per shard)", len(fired), len(shards))
	}
	seen := map[string]bool{}
	for i := 0; i < len(shards); i++ {
		id := <-fired
		seen[id] = true
		// job must have completed in the store
		j, err := s.Get(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if j.State != Completed {
			t.Errorf("job %s (shard %s) state = %s", id, ids[id], j.State)
		}
	}
	if len(seen) != len(shards) {
		t.Fatal("duplicate delivery across shards")
	}
}

func TestClusterQuiescenceWatermarkIsPerShard(t *testing.T) {
	// each shard engine keeps its own watermark; a far-future job in
	// shard 3 must not silence shard 7
	s := newTestStore(t)
	ctx := context.Background()
	base := time.Date(2026, 11, 1, 9, 0, 0, 0, time.UTC)
	clk := &fakeClock{now: base}

	c := newCluster(s, clk, engineConfig{lookahead: time.Minute, admitLimit: 100},
		[]ScanUnit{ScanUnit("3"), ScanUnit("7")})
	defer c.Close()

	// far-future job in shard 3 -> shard 3's watermark set
	s.Insert(ctx, &Job{Topic: "t", Due: base.Add(6 * time.Hour), State: Waiting, Shard: 3})
	c.stepAll(ctx)
	e3 := c.engineFor(ScanUnit("3"))
	e7 := c.engineFor(ScanUnit("7"))
	if e3.quietUntilNow().Before(clk.Now()) {
		t.Fatal("shard-3 watermark not set")
	}
	if e7.quietUntilNow().After(clk.Now()) {
		t.Fatal("shard-7 watermark polluted by shard-3's far-future job")
	}
}

func shardOf(u ScanUnit) int {
	n := 0
	for _, ch := range string(u) {
		n = n*10 + int(ch-'0')
	}
	return n
}

var _ = sync.Mutex{}
