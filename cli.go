package hourglass

import (
	"context"
	"time"
)

// Stats is the store-level overview for `hourglass stats`.
type Stats struct {
	JobsByState    []StateCount
	TotalJobs      int64
	TotalReceipts  int64
	OldestTerminal time.Time // oldest terminal row still kept (retention age)
}

// StateCount is one row of the by-state breakdown.
type StateCount struct {
	State State
	Count int64
}

// Stats summarizes the store: job counts by state, receipt total, and
// how long the oldest terminal row has been kept (retention pressure).
func (h *Handle) Stats(ctx context.Context) (*Stats, error) {
	s := h.eng.storeStats(ctx)
	return s, nil
}

// ListDead lists dead-lettered jobs, newest first, optionally filtered
// by topic, for `hourglass dlq ls`.
func (h *Handle) ListDead(ctx context.Context, topic string, limit int) ([]Job, error) {
	return h.eng.listDead(ctx, topic, limit)
}

// ReplayDead requeues one dead job: dead -> waiting, attempts reset,
// due = now. The engine's next admission picks it up.
func (h *Handle) ReplayDead(ctx context.Context, id string) error {
	return h.eng.replayDead(ctx, id)
}

// ReplayAllDead requeues every dead job; returns the count.
func (h *Handle) ReplayAllDead(ctx context.Context) (int, error) {
	return h.eng.replayAllDead(ctx)
}
