package hourglass

import (
	"context"
	"time"
)

// Store is the persistence seam: the database keeps the
// long horizon. SQLite implements one scan unit (embedded); MySQL/TiDB
// implement shard-scoped scans plus lease reclaim (cluster).
type Store interface {
	Ping(ctx context.Context) error
	Insert(ctx context.Context, j *Job) error
	Get(ctx context.Context, id string) (*Job, error)
	// Select waiting jobs in [from, to] ordered by due time, limit n.
	Select(ctx context.Context, unit ScanUnit, from, to time.Time, n int) ([]Job, error)
	// Acquire CAS's waiting -> acquired for exactly these ids, under
	// this epoch; it returns only the winners, re-read from the store.
	Acquire(ctx context.Context, unit ScanUnit, epoch uint64, ids []string) ([]Job, error)

	// -- engine behaviors (both drivers implement all of these) --
	transition(ctx context.Context, id string, from, to State) error
	transitionBatch(ctx context.Context, from, to State, ids []string, at time.Time) error
	setDue(ctx context.Context, id string, due time.Time) error
	cancel(ctx context.Context, id, reason string) error
	minPendingDueIn(ctx context.Context, unit ScanUnit) (time.Time, bool, error)
	attemptFor(ctx context.Context, ids []string) error
	attemptForRollback(ctx context.Context, ids []string) error
	getMany(ctx context.Context, ids []string) (map[string]Job, error)
	addReceiptSingle(ctx context.Context, jobID, kind, errText, reason, state string, at time.Time) error
	addReceiptAttempt(ctx context.Context, ids []string, jobs map[string]Job, kind string, at time.Time) error
	addReceiptAttemptErr(ctx context.Context, ids []string, jobs map[string]Job, kind, errText string, at time.Time) error
	retryFor(ctx context.Context, id string, now time.Time, backoff time.Duration) error
}

// StoreExt is the cluster-only extension: lease-based reclaim (the
// shared database as arbiter; etcd only owns epochs).
type StoreExt interface {
	Store
	Reclaim(ctx context.Context, unit ScanUnit, epoch uint64, ttl time.Duration) (int, error)
}
