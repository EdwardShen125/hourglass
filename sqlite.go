package hourglass

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"
	"sync/atomic"
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
	s := &sqliteStore{db: &countingDB{DB: db}}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

type sqliteStore struct {
	db *countingDB
}

// countingDB counts write statements (ExecContext) so tests can assert
// batching behavior: one tick's fire path must be O(batches) writes,
// not O(jobs). Reads are not counted — waiting loops poll with SELECTs.
type countingDB struct {
	*sql.DB
	stmts atomic.Int64
}

func (c *countingDB) ExecContext(ctx context.Context, q string, args ...any) (sql.Result, error) {
	c.stmts.Add(1)
	return c.DB.ExecContext(ctx, q, args...)
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
	attempts INTEGER NOT NULL DEFAULT 0,   -- times fired
	max_attempts INTEGER NOT NULL DEFAULT 1,
	backoff  INTEGER NOT NULL DEFAULT 30, -- seconds
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
		`INSERT INTO jobs (id, topic, payload, due, sched_at, state, cron, attempts, max_attempts, backoff, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		j.ID, j.Topic, j.Payload, j.Due.Unix(), j.ScheduledAt.Unix(), string(j.State), j.Cron, j.Attempts, j.MaxAttempts, int64(j.Backoff/time.Second), time.Now().Unix())
	return err
}

func (s *sqliteStore) Get(ctx context.Context, id string) (*Job, error) {
	row := s.db.QueryRowContext(ctx, `SELECT id, topic, payload, due, sched_at, state, cron, attempts, max_attempts, backoff, cancel_reason FROM jobs WHERE id = ?`, id)
	var j Job
	var state string
	var due int64
	var payload []byte
	var sched, attempts, maxAttempts, backoff int64
	if err := row.Scan(&j.ID, &j.Topic, &payload, &due, &sched, &state, &j.Cron, &attempts, &maxAttempts, &backoff, &j.CancelReason); err != nil {
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
	j.Attempts, j.MaxAttempts = int(attempts), int(maxAttempts)
	j.Backoff = time.Duration(backoff) * time.Second
	j.State = State(state)
	return &j, nil
}

// Select implements Store: waiting jobs in [from, to] by due order.
func (s *sqliteStore) Select(ctx context.Context, unit ScanUnit, from, to time.Time, n int) ([]Job, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, topic, payload, due, sched_at, state, cron, attempts, max_attempts, backoff, cancel_reason FROM jobs
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
	sb.WriteString(`SELECT id, topic, payload, due, sched_at, state, cron, attempts, max_attempts, backoff, cancel_reason FROM jobs WHERE state = 'acquired' AND id IN (`)
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
		var due, sched, attempts, maxAttempts, backoff int64
		if err := rows.Scan(&j.ID, &j.Topic, &j.Payload, &due, &sched, &state, &j.Cron, &attempts, &maxAttempts, &backoff, &j.CancelReason); err != nil {
			return nil, err
		}
		j.Due = time.Unix(due, 0).UTC()
		if sched != 0 {
			j.ScheduledAt = time.Unix(sched, 0).UTC()
		}
		j.Attempts, j.MaxAttempts = int(attempts), int(maxAttempts)
		j.Backoff = time.Duration(backoff) * time.Second
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

// retryFor requeues a fired row for its next attempt: waiting with
// due=now+backoff. The attempt counter was already bumped at fire time
// (attemptFor); this only moves the row across the edge.
func (s *sqliteStore) retryFor(ctx context.Context, id string, now time.Time, backoff time.Duration) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE jobs SET state='waiting', due=? WHERE id=? AND state='fired'`,
		now.Add(backoff).Unix(), id)
	return err
}

// attemptFor bumps the attempt counter at fire time.
func (s *sqliteStore) attemptFor(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	ph := strings.Repeat("?,", len(ids))
	q := `UPDATE jobs SET attempts=attempts+1 WHERE state='fired' AND id IN (` + ph[:len(ph)-1] + `)`
	args := make([]any, 0, len(ids))
	for _, id := range ids {
		args = append(args, id)
	}
	_, err := s.db.ExecContext(ctx, q, args...)
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

// transitionBatch advances many rows along one legal edge in a single
// UPDATE ... WHERE state = from AND id IN (...). Rows not in from are
// skipped silently (races lose). Returns nothing: the engine treats
// skipped rows as gone (canceled meanwhile).
func (s *sqliteStore) transitionBatch(ctx context.Context, from, to State, ids []string, at time.Time) error {
	if len(ids) == 0 {
		return nil
	}
	if !legalEdges[[2]State{from, to}] {
		return fmt.Errorf("hourglass: illegal edge %s -> %s (batch of %d)", from, to, len(ids))
	}
	ph := strings.Repeat("?,", len(ids))
	q := `UPDATE jobs SET state = ? WHERE state = ? AND id IN (` + ph[:len(ph)-1] + `)`
	args := make([]any, 0, len(ids)+2)
	args = append(args, string(to), string(from))
	for _, id := range ids {
		args = append(args, id)
	}
	_, err := s.db.ExecContext(ctx, q, args...)
	return err
}

// stmtCount exposes the statement counter (tests only).
func (s *sqliteStore) listAll(ctx context.Context) ([]Job, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, topic, payload, due, sched_at, state, cron, attempts, max_attempts, backoff, cancel_reason FROM jobs`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanJobs(rows)
}

// getMany fetches job rows by id set in one statement (pump re-read).
func (s *sqliteStore) getMany(ctx context.Context, ids []string) (map[string]Job, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	ph := strings.Repeat("?,", len(ids))
	q := `SELECT id, topic, payload, due, sched_at, state, cron, attempts, max_attempts, backoff, cancel_reason FROM jobs WHERE id IN (` + ph[:len(ph)-1] + `)`
	args := make([]any, 0, len(ids))
	for _, id := range ids {
		args = append(args, id)
	}
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]Job, len(ids))
	list, err := scanJobs(rows)
	if err != nil {
		return nil, err
	}
	for _, j := range list {
		out[j.ID] = j
	}
	return out, nil
}

func (s *sqliteStore) stmtCount() int64 { return s.db.stmts.Load() }

func newID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("hourglass: crypto/rand unavailable: %v", err))
	}
	return hex.EncodeToString(b)
}
