package hourglass

import (
	"context"
	"database/sql"
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
	state    TEXT NOT NULL DEFAULT 'waiting',
	epoch    INTEGER NOT NULL DEFAULT 0,
	created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_jobs_due_state ON jobs(due, state);
`

func (s *sqliteStore) migrate() error {
	_, err := s.db.Exec(schema)
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
		`INSERT INTO jobs (id, topic, payload, due, state, created_at) VALUES (?, ?, ?, ?, ?, ?)`,
		j.ID, j.Topic, j.Payload, j.Due.Unix(), string(j.State), time.Now().Unix())
	return err
}

func (s *sqliteStore) Get(ctx context.Context, id string) (*Job, error) {
	row := s.db.QueryRowContext(ctx, `SELECT id, topic, payload, due, state FROM jobs WHERE id = ?`, id)
	var j Job
	var state string
	var due int64
	var payload []byte
	if err := row.Scan(&j.ID, &j.Topic, &payload, &due, &state); err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("hourglass: job %q not found", id)
		}
		return nil, err
	}
	j.Payload = payload
	j.Due = time.Unix(due, 0).UTC()
	j.State = State(state)
	return &j, nil
}

// Select implements Store: waiting jobs in [from, to] by due order.
func (s *sqliteStore) Select(ctx context.Context, unit ScanUnit, from, to time.Time, n int) ([]Job, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, topic, payload, due, state FROM jobs
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
	sb.WriteString(`SELECT id, topic, payload, due, state FROM jobs WHERE state = 'acquired' AND id IN (`)
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
		var due int64
		if err := rows.Scan(&j.ID, &j.Topic, &j.Payload, &due, &state); err != nil {
			return nil, err
		}
		j.Due = time.Unix(due, 0).UTC()
		j.State = State(state)
		out = append(out, j)
	}
	return out, rows.Err()
}

// setState force-sets a job's state (engine transitions; v1 minimal
// read-modify-write on the single-writer store).
func (s *sqliteStore) setState(ctx context.Context, id string, st State) error {
	_, err := s.db.ExecContext(ctx, `UPDATE jobs SET state = ? WHERE id = ?`, string(st), id)
	return err
}

func newID() string {
	// TODO: random id. For now, time-based to keep cycle 1 minimal.
	return fmt.Sprintf("j-%d", time.Now().UnixNano())
}
