package hourglass

import (
	"context"
	"strings"
	"time"
)

// Receipt is one durable execution-log row: every state transition of
// interest is a row the user can SELECT. "Did this side effect happen?"
// is a query on every deployment shape (DESIGN.md).
type Receipt struct {
	ID      string
	JobID   string
	Kind    string // fired | completed | dead | canceled
	Attempt int
	Error   string
	Reason  string // cancel reason
	At      time.Time
}

const receiptsSchema = `
CREATE TABLE IF NOT EXISTS receipts (
	id       TEXT PRIMARY KEY,
	job_id   TEXT NOT NULL,
	kind     TEXT NOT NULL,
	attempt  INTEGER NOT NULL DEFAULT 1,
	error    TEXT NOT NULL DEFAULT '',
	reason   TEXT NOT NULL DEFAULT '',
	at       INTEGER NOT NULL,           -- unix seconds (UTC)
	state    TEXT NOT NULL DEFAULT ''    -- job state after the event (denormalized for queries)
);
CREATE INDEX IF NOT EXISTS idx_receipts_job ON receipts(job_id, at);
`

func (s *sqliteStore) addReceipt(ctx context.Context, jobID, kind, errText, reason, state string, at time.Time) error {
	return s.addReceiptBatch(ctx, []string{jobID}, kind, errText, reason, state, at)
}

// addReceiptBatch writes one receipt per job in a single multi-VALUES
// INSERT: the hot minute must not be a receipt write storm.
func (s *sqliteStore) addReceiptBatch(ctx context.Context, jobIDs []string, kind, errText, reason, state string, at time.Time) error {
	if len(jobIDs) == 0 {
		return nil
	}
	var sb strings.Builder
	sb.WriteString(`INSERT INTO receipts (id, job_id, kind, attempt, error, reason, at, state) VALUES `)
	args := make([]any, 0, len(jobIDs)*8)
	for i, jobID := range jobIDs {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString("(?, ?, ?, 1, ?, ?, ?, ?)")
		args = append(args, newID(), jobID, kind, errText, reason, at.Unix(), state)
	}
	_, err := s.db.ExecContext(ctx, sb.String(), args...)
	return err
}

func (s *sqliteStore) receipts(ctx context.Context, jobID string) ([]Receipt, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, job_id, kind, attempt, error, reason, at FROM receipts WHERE job_id = ? ORDER BY at ASC, rowid ASC`, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Receipt
	for rows.Next() {
		var r Receipt
		var at int64
		if err := rows.Scan(&r.ID, &r.JobID, &r.Kind, &r.Attempt, &r.Error, &r.Reason, &at); err != nil {
			return nil, err
		}
		r.At = time.Unix(at, 0).UTC()
		out = append(out, r)
	}
	return out, rows.Err()
}
