package hourglass

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

func newSQLiteStore(path string) (*sqliteStore, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("hourglass: open sqlite %s: %w", path, err)
	}
	// Single-writer store: one connection serializes writes and keeps
	// SQLite honest under concurrent admission.
	db.SetMaxOpenConns(1)
	s := &sqliteStore{db: db}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

type sqliteStore struct {
	db *sql.DB
}

const schema = `
CREATE TABLE IF NOT EXISTS jobs (
	id       TEXT PRIMARY KEY,
	topic    TEXT NOT NULL,
	payload  BLOB,
	due      INTEGER NOT NULL,          -- unix seconds (UTC)
	sched_at INTEGER NOT NULL DEFAULT 0, -- pre-jitter time (== due when no jitter)
	state    TEXT NOT NULL DEFAULT 'waiting',
	cron     TEXT NOT NULL DEFAULT '',
	cancel_reason TEXT NOT NULL DEFAULT '',
	epoch    INTEGER NOT NULL DEFAULT 0,
	created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_jobs_due_state ON jobs(due, state);
`

func (s *sqliteStore) migrate() error {
	_, err := s.db.Exec(schema + receiptsSchema)
	if err != nil {
		return fmt.Errorf("hourglass: migrate: %w", err)
	}
	return nil
}

func (s *sqliteStore) Ping(ctx context.Context) error {
	var one int
	return s.db.QueryRowContext(ctx, "SELECT 1").Scan(&one)
}

func (s *sqliteStore) Insert(ctx context.Context, j *Job) error {
	if j.ID == "" {
		j.ID = newID()
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO jobs (id, topic, payload, due, sched_at, state, cron, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		j.ID, j.Topic, j.Payload, j.Due.Unix(), j.ScheduledAt.Unix(), string(j.State), j.Cron, time.Now().Unix())
	return err
}

func (s *sqliteStore) Get(ctx context.Context, id string) (*Job, error) {
	row := s.db.QueryRowContext(ctx, `SELECT id, topic, payload, due, sched_at, state, cron, cancel_reason FROM jobs WHERE id = ?`, id)
	var j Job
	var state string
	var due int64
	var payload []byte
	var sched int64
	if err := row.Scan(&j.ID, &j.Topic, &payload, &due, &sched, &state, &j.Cron, &j.CancelReason); err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("hourglass: job %q not found", id)
		}
		return nil, err
	}
	j.Payload = payload
	j.Due = time.Unix(due, 0).UTC()
	if sched != 0 {
		j.ScheduledAt = time.Unix(sched, 0).UTC()
	}
	j.State = State(state)
	return &j, nil
}

// Select implements Store: waiting jobs in [from, to] by due order.
func (s *sqliteStore) Select(ctx context.Context, unit ScanUnit, from, to time.Time, n int) ([]Job, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, topic, payload, due, sched_at, state, cron, cancel_reason FROM jobs
		 WHERE state = 'waiting' AND due >= ? AND due <= ?
		 ORDER BY due ASC LIMIT ?`, from.Unix(), to.Unix(), n)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanJobs(rows)
}

// Acquire implements Store: CAS waiting -> acquired for these ids.
func (s *sqliteStore) Acquire(ctx context.Context, unit ScanUnit, epoch uint64, ids []string) ([]Job, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	placeholders := strings.Repeat("?,", len(ids))
	q := `UPDATE jobs SET state = 'acquired', epoch = ? WHERE state = 'waiting' AND id IN (` +
		placeholders[:len(placeholders)-1] + `)`
	args := make([]any, 0, len(ids)+1)
	args = append(args, epoch)
	for _, id := range ids {
		args = append(args, id)
	}
	res, err := s.db.ExecContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	won, _ := res.RowsAffected()
	// Re-read winners: the CAS'd rows are the truth, not the caller's
	// in-memory copies.
	if won == 0 {
		return nil, nil
	}
	var sb strings.Builder
	sb.WriteString(`SELECT id, topic, payload, due, sched_at, state, cron, cancel_reason FROM jobs WHERE state = 'acquired' AND id IN (`)
	for i := range ids {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString("?")
	}
	sb.WriteString(`) ORDER BY due ASC`)
	rargs := make([]any, 0, len(ids))
	for _, id := range ids {
		rargs = append(rargs, id)
	}
	rows, err := s.db.QueryContext(ctx, sb.String(), rargs...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanJobs(rows)
}

func scanJobs(rows *sql.Rows) ([]Job, error) {
	var out []Job
	for rows.Next() {
		var j Job
		var state string
		var due, sched int64
		if err := rows.Scan(&j.ID, &j.Topic, &j.Payload, &due, &sched, &state, &j.Cron, &j.CancelReason); err != nil {
			return nil, err
		}
		j.Due = time.Unix(due, 0).UTC()
		if sched != 0 {
			j.ScheduledAt = time.Unix(sched, 0).UTC()
		}
		j.State = State(state)
		out = append(out, j)
	}
	return out, rows.Err()
}

// setDue updates the due time (cron reschedule).
func (s *sqliteStore) setDue(ctx context.Context, id string, due time.Time) error {
	_, err := s.db.ExecContext(ctx, `UPDATE jobs SET due=? WHERE id=?`, due.Unix(), id)
	return err
}

// cancel flips a waiting/acquired row to canceled with a reason; the
// row persists as a receipt until retention expires.
func (s *sqliteStore) cancel(ctx context.Context, id, reason string) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE jobs SET state='canceled', cancel_reason=? WHERE id=? AND state IN ('waiting','acquired')`, reason, id)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return fmt.Errorf("hourglass: job %s no longer cancelable (raced with fire)", id)
	}
	return nil
}

// reschedule moves a completed cron job to waiting with its next due.
func (s *sqliteStore) reschedule(ctx context.Context, id string, next time.Time) error {
	_, err := s.db.ExecContext(ctx, `UPDATE jobs SET state='waiting', due=? WHERE id=? AND state='completed'`, next.Unix(), id)
	return err
}

// legalEdges is the DESIGN.md state machine: every allowed transition.
var legalEdges = map[[2]State]bool{
	{Waiting, Acquired}:  true, // CAS admission
	{Acquired, Fired}:    true, // wheel fired, handler dispatched
	{Fired, Completed}:   true, // handler success
	{Fired, Dead}:        true, // retries exhausted (v1: single attempt)
	{Waiting, Canceled}:  true, // strong cancel, before admission
	{Acquired, Canceled}: true, // wheel mode, before fire
	{Acquired, Waiting}:  true, // wheel overflow release (anti-loss)
	{Completed, Waiting}: true, // reschedule: cron next occurrence
}

// transition advances state atomically: UPDATE ... WHERE state = from,
// and only along a legal edge of the state machine. Illegal edges and
// lost races fail cleanly instead of corrupting.
func (s *sqliteStore) transition(ctx context.Context, id string, from, to State) error {
	if !legalEdges[[2]State{from, to}] {
		return fmt.Errorf("hourglass: illegal edge %s -> %s for %s", from, to, id)
	}
	res, err := s.db.ExecContext(ctx, `UPDATE jobs SET state = ? WHERE id = ? AND state = ?`, string(to), id, string(from))
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return fmt.Errorf("hourglass: transition %s -> %s for %s: row not in %s (lost race or illegal edge)", from, to, id, from)
	}
	return nil
}

func newID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("hourglass: crypto/rand unavailable: %v", err))
	}
	return hex.EncodeToString(b)
}
