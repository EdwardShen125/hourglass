package hourglass

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// Standard testing.B benchmarks on the embedded engine (SQLite, no
// external services) so that plain
//
//	go test -bench 'Benchmark(Submit|Fire)' -count 10 > old.txt
//	# ... change something ...
//	go test -bench 'Benchmark(Submit|Fire)' -count 10 > new.txt
//	benchstat old.txt new.txt
//
// yields statistically compared regressions anywhere, CI included.
// Cluster scenarios (real MySQL/etcd) stay in cmd/hourglass-bench.

// BenchmarkSubmit measures the durable-submit path: one INSERT round
// trip per job, by design — no ack without the row.
func BenchmarkSubmit(b *testing.B) {
	h, err := Open("sqlite://" + filepath.Join(b.TempDir(), "bench.db"))
	if err != nil {
		b.Fatal(err)
	}
	defer h.Close()
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := h.Submit(ctx, "bench", nil, At(time.Now().UTC().Add(time.Hour))); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkFirePath measures the batched fire pipeline end to end:
// admission (select + CAS), wheel tick, dispatch, batched terminal
// transitions and the receipt batch — the path a hot minute actually
// exercises. One op = `batch` jobs through the full pipeline; the
// jobs/op metric keeps that explicit in benchstat output.
func BenchmarkFirePath(b *testing.B) {
	const batch = 1000
	h, err := Open("sqlite://" + filepath.Join(b.TempDir(), "bench.db"))
	if err != nil {
		b.Fatal(err)
	}
	defer h.Close()

	var fired atomic.Int64
	h.Register("bench", func(context.Context, Job) error {
		fired.Add(1)
		return nil
	})
	ctx := context.Background()
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	go h.Run(runCtx)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fired.Store(0)
		target := fired.Load() + batch
		now := time.Now().UTC()
		for j := 0; j < batch; j++ {
			if _, err := h.Submit(ctx, "bench", nil, At(now.Add(20*time.Millisecond))); err != nil {
				b.Fatal(err)
			}
		}
		deadline := time.Now().Add(30 * time.Second)
		for fired.Load() < target && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		if fired.Load() < target {
			b.Fatalf("fired %d < %d", fired.Load(), target)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(batch), "jobs/op")
}
