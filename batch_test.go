package hourglass

import (
	"context"
	"testing"
	"time"
)

// batched transitions: one round-trip per tick per target state.
func TestTransitionBatchCAS(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	base := time.Date(2026, 11, 1, 9, 0, 0, 0, time.UTC)

	ids := []string{
		mustSubmit(t, s, "a", base),
		mustSubmit(t, s, "b", base.Add(time.Second)),
		mustSubmit(t, s, "c", base.Add(2*time.Second)),
	}
	// one id will be in the wrong from-state: it must be skipped, not
	// block the batch
	wrongFrom := mustSubmit(t, s, "d", base.Add(3*time.Second))
	if _, err := s.Acquire(ctx, scanUnitAll, 1, []string{wrongFrom}); err != nil {
		t.Fatal(err)
	}

	// waiting -> acquired for [a b c d]: a,b,c win; d is not waiting
	won, err := s.Acquire(ctx, scanUnitAll, 1, ids[:3])
	if err != nil {
		t.Fatal(err)
	}
	if len(won) != 3 {
		t.Fatalf("won %d, want 3", len(won))
	}

	// batch fire: acquired -> fired, one statement
	now := base.Add(time.Minute)
	if err := s.transitionBatch(ctx, Acquired, Fired, ids[:3], now); err != nil {
		t.Fatalf("transitionBatch: %v", err)
	}
	for _, id := range ids[:3] {
		j, _ := s.Get(ctx, id)
		if j.State != Fired {
			t.Errorf("%s state = %s, want fired", id, j.State)
		}
	}

	// batch complete: fired -> completed
	if err := s.transitionBatch(ctx, Fired, Completed, ids[:3], now); err != nil {
		t.Fatal(err)
	}

	// d (still acquired) is untouched by the fired->completed batch
	j, _ := s.Get(ctx, wrongFrom)
	if j.State != Acquired {
		t.Errorf("wrong-from row mutated: %s", j.State)
	}

	// a second batch fire of the same ids wins nothing: rows are
	// completed now, not acquired
	if err := s.transitionBatch(ctx, Acquired, Fired, ids[:3], now); err != nil {
		t.Fatal(err)
	}
	j, _ = s.Get(ctx, ids[0])
	if j.State != Completed {
		t.Errorf("re-fired completed row: state = %s", j.State)
	}
}

func TestFirePathWritesBatchedReceiptsAndTransitions(t *testing.T) {
	// engine fires N jobs in one pump; transitions, receipts, and the
	// handler feed must be batched. Statements are counted between the
	// pump's start and the flush callback (no polling pollution).
	e, clk := newTestEngine(t)
	ctx := context.Background()
	e.Register("job", func(context.Context, Job) error { return nil })

	const n = 50
	for i := 0; i < n; i++ {
		if _, err := e.Submit(ctx, "job", nil, At(clk.Now().Add(time.Duration(i+1)*time.Second))); err != nil {
			t.Fatal(err)
		}
	}
	e.step(ctx)

	flushed := make(chan struct{})
	before := e.stmtCount()
	e.pump(clk.Now().Add(2 * time.Minute))
	// the flush callback fires after the last handler outcome; hook it by
	// waiting for all jobs completed via receipts count = 2n (single
	// query poll, coarse).
	// wait until every job has both receipts (fired + completed); FAIL on
	// timeout instead of silently proceeding (review: vacuous waits)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if allJobsHaveTwoReceipts(t, e, ctx, n) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !allJobsHaveTwoReceipts(t, e, ctx, n) {
		t.Fatal("flush did not complete within 10s (batch wedged or spilled without re-admission)")
	}
	// under -race the workers run slower; the countingDB atomic is fine
	// but each poll's COUNT(*) is counted. 225 - ~4N polls... instead of
	// fighting poll noise, freeze measurement: count only ExecContext
	// (writes), not reads.
	writes := e.stmtCount() - before
	close(flushed)

	// write ceiling (writes only, reads excluded): pump = 1 batch
	// transition + 1 receipt batch + flush(1 completed transition + 1
	// receipt batch) = 4 writes. Cron reschedules would add per-job
	// writes; this test's jobs have no cron. Unbatched = 2N+ (100+).
	if writes >= 20 {
		t.Fatalf("pump for %d jobs issued %d write statements; batching not effective", n, writes)
	}
	_ = flushed
	for _, id := range jobIDs(t, e) {
		rs, err := e.store.(*sqliteStore).receipts(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if len(rs) != 2 {
			t.Fatalf("job %s: %d receipts, want 2", id, len(rs))
		}
	}
}

func allJobsHaveTwoReceipts(t *testing.T, e *engine, ctx context.Context, want int) bool {
	t.Helper()
	ids := jobIDs(t, e)
	if len(ids) != want {
		return false
	}
	for _, id := range ids {
		rs, _ := e.store.(*sqliteStore).receipts(ctx, id)
		if len(rs) < 2 {
			return false
		}
	}
	return true
}

func jobIDs(t *testing.T, e *engine) []string {
	t.Helper()
	js, err := e.store.(*sqliteStore).listAll(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]string, 0, len(js))
	for _, j := range js {
		ids = append(ids, j.ID)
	}
	return ids
}
