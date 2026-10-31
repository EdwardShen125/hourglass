package hourglass

import (
	"context"
	"time"
)

// Store is the persistence seam from DESIGN.md: the database keeps the
// long horizon. SQLite implements one scan unit; MySQL/TiDB will
// implement one per partition.
type Store interface {
	Ping(ctx context.Context) error
	Insert(ctx context.Context, j *Job) error
	Get(ctx context.Context, id string) (*Job, error)
	// Select waiting jobs in [from, to] ordered by due time, limit n.
	Select(ctx context.Context, unit ScanUnit, from, to time.Time, n int) ([]Job, error)
	// Acquire CAS's waiting -> acquired for exactly these ids, under
	// this epoch; it returns only the winners, re-read from the store.
	Acquire(ctx context.Context, unit ScanUnit, epoch uint64, ids []string) ([]Job, error)
}
