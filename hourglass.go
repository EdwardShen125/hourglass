// Package hourglass provides delayed jobs and cron for Go: durable truth
// in SQL, second-level fire precision from an in-process hierarchical time
// wheel. See README ("How it works") for the timing contract.
package hourglass

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// ScanUnit is one unit of admission scanning: the whole table for
// SQLite, one partition for MySQL/TiDB.
type ScanUnit string

// scanUnitAll is SQLite's single scan unit.
const scanUnitAll ScanUnit = "all"

// State is the lifecycle state of a job.
type State string

const (
	// Waiting: admitted by nobody yet; the quiet row.
	Waiting State = "waiting"
	// Acquired: CAS'd by an admission loop; held in the wheel.
	Acquired State = "acquired"
	// Fired: the wheel fired; handler dispatched.
	Fired State = "fired"
	// Completed: terminal success.
	Completed State = "completed"
	// Dead: terminal failure; dead-lettered.
	Dead State = "dead"
	// Canceled: revoked before fire; persists as a receipt.
	Canceled State = "canceled"
)

// Job is one durable delayed job.
type Job struct {
	ID      string
	Topic   string
	Payload []byte
	Due     time.Time
	State   State
	Cron    string // non-empty: recurring; reschedules on completion
	// ScheduledAt is the pre-jitter time; Due is what the wheel obeys.
	ScheduledAt time.Time

	Attempts    int           // fires so far (including the current one)
	MaxAttempts int           // 1 = no retries; exhausted -> dead
	Backoff     time.Duration // delay before the next attempt

	Shard int // shard key (hash placement, computed once at submit)

	// CreatedAt is the submit time (persisted; used by stores).
	CreatedAt time.Time

	CancelReason string // set when State == canceled
}

// Handle is an hourglass engine bound to one store.
type Handle struct {
	db    *countingDB
	store Store
	eng   *engine
	clk   clock
}

// Open connects to a store named by dsn, e.g. "sqlite:///path/to/db".
func Open(dsn string) (*Handle, error) {
	return openWithClock(dsn, realClock{})
}

// Clock is the time seam; tests inject a deterministic clock.
type Clock interface {
	Now() time.Time
}

// OpenWithClock is Open with an injected clock (deterministic tests,
// simulated time).
func OpenWithClock(dsn string, clk Clock) (*Handle, error) {
	return openWithClock(dsn, clk)
}

func openWithClock(dsn string, clk Clock) (*Handle, error) {
	c := clock(clk)
	switch {
	case strings.HasPrefix(dsn, "sqlite://"):
		path := strings.TrimPrefix(dsn, "sqlite://")
		store, err := newSQLiteStore(path)
		if err != nil {
			return nil, err
		}
		eng := newEngine(store, c, defaultEngineConfig())
		return &Handle{db: store.db, store: store, eng: eng, clk: c}, nil
	default:
		return nil, fmt.Errorf("hourglass: unsupported dsn %q (only sqlite:// supported)", dsn)
	}
}

// Ping verifies the store is reachable and migrated.
func (h *Handle) Ping(ctx context.Context) error { return h.store.Ping(ctx) }

// Get fetches one job by id.
func (h *Handle) Get(ctx context.Context, id string) (*Job, error) { return h.eng.Get(ctx, id) }

// Register binds a handler to a topic.
func (h *Handle) Register(topic string, fn Handler) { h.eng.Register(topic, fn) }

// Run drives the engine until ctx is done: admission steps on a one-
// second cadence, wheel ticks between them. Blocks; run it in a
// goroutine.
func (h *Handle) Run(ctx context.Context) {
	h.eng.Run(ctx)
}

// Cancel revokes a job before its fire moment; the row persists as a
// receipt. Cancel after fire is rejected.
func (h *Handle) Cancel(ctx context.Context, id, reason string) error {
	return h.eng.Cancel(ctx, id, reason)
}

// Every schedules a recurring job from a standard 5-field cron
// expression; the same job row reschedules after each completion.
func (h *Handle) Every(ctx context.Context, topic, spec string) (string, error) {
	return h.eng.Every(ctx, topic, spec)
}

// Eng exposes the internal engine for package tests (CLI integration
// drives admission deterministically). Not part of the stable API.
func (h *Handle) Eng() *engine { return h.eng }

// ErrEmptyTopic and ErrMissingDue are submit-time contract violations.
var (
	ErrEmptyTopic = errors.New("hourglass: empty topic")
	ErrMissingDue = errors.New("hourglass: no due time (use At or Delay)")
)

// SubmitOption mutates a job before it is persisted.
type SubmitOption func(*submitConfig)

type submitConfig struct {
	due         time.Time
	hasDue      bool
	jitter      time.Duration
	maxAttempts int
	backoff     time.Duration
	shard       int
	shardSet    bool
}

// At schedules the job at an exact time.
func At(t time.Time) SubmitOption {
	return func(c *submitConfig) { c.due, c.hasDue = t, true }
}

// Delay schedules the job relative to submit time.
func Delay(d time.Duration) SubmitOption {
	return func(c *submitConfig) { c.due, c.hasDue = time.Now().Add(d), true }
}

// Jitter disperses the due time by rand(0, w), drawn once at submit and
// stored: hot minutes melt before they form (README). Default zero.
func Jitter(w time.Duration) SubmitOption {
	return func(c *submitConfig) { c.jitter = w }
}

// Retries sets additional attempts after the first failure.
// Retries(2) = 3 total attempts. Default 0 (single attempt).
func Retries(n int) SubmitOption {
	return func(c *submitConfig) { c.maxAttempts = 1 + n }
}

// InShard forces the shard placement (tests, ops tooling; default is
// hash(id)).
func InShard(n int) SubmitOption {
	return func(c *submitConfig) { c.shard, c.shardSet = n, true }
}

// RetryBackoff sets the delay before a failed attempt refires.
// Default 30s. The delay is constant; exponential growth is a v1.x knob.
func RetryBackoff(d time.Duration) SubmitOption {
	return func(c *submitConfig) { c.backoff = d }
}

// Submit persists a job and returns its id.
func (h *Handle) Submit(ctx context.Context, topic string, payload []byte, opts ...SubmitOption) (string, error) {
	return h.eng.Submit(ctx, topic, payload, opts...)
}

// Close stops the engine (workers, wheel, in-flight flushes) and
// releases the store.
func (h *Handle) Close() error {
	if err := h.eng.Close(); err != nil {
		h.db.Close()
		return err
	}
	return h.db.Close()
}
