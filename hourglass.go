// Package hourglass provides delayed jobs and cron for Go: durable truth
// in SQL, second-level fire precision from an in-process hierarchical time
// wheel. See DESIGN.md for the timing contract.
package hourglass

import (
	"context"
	"database/sql"
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

	CancelReason string // set when State == canceled
}

// Handle is an hourglass engine bound to one store.
type Handle struct {
	db    *sql.DB
	store Store
	eng   *engine
	clk   clock
}

// Open connects to a store named by dsn, e.g. "sqlite:///path/to/db".
func Open(dsn string) (*Handle, error) {
	switch {
	case strings.HasPrefix(dsn, "sqlite://"):
		path := strings.TrimPrefix(dsn, "sqlite://")
		store, err := newSQLiteStore(path)
		if err != nil {
			return nil, err
		}
		clk := realClock{}
		eng := newEngine(store, clk, defaultEngineConfig())
		return &Handle{db: store.db, store: store, eng: eng, clk: clk}, nil
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

// ErrEmptyTopic and ErrMissingDue are submit-time contract violations.
var (
	ErrEmptyTopic = errors.New("hourglass: empty topic")
	ErrMissingDue = errors.New("hourglass: no due time (use At or Delay)")
)

// SubmitOption mutates a job before it is persisted.
type SubmitOption func(*submitConfig)

type submitConfig struct {
	due    time.Time
	hasDue bool
	jitter time.Duration
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
// stored: hot minutes melt before they form (DESIGN.md). Default zero.
func Jitter(w time.Duration) SubmitOption {
	return func(c *submitConfig) { c.jitter = w }
}

// Submit persists a job and returns its id.
func (h *Handle) Submit(ctx context.Context, topic string, payload []byte, opts ...SubmitOption) (string, error) {
	return h.eng.Submit(ctx, topic, payload, opts...)
}

// Close releases the store.
func (h *Handle) Close() error { return h.db.Close() }
