package hourglass

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// mysqlStore is the cluster-mode Store: same schema and semantics as
// SQLite, plus the two cluster-only mechanisms the shared database
// provides — shard-scoped scans and lease-based reclaim (river-style:
// acquired_at aging, no coordinator notification needed; etcd only owns
// epochs).
type mysqlStore struct {
	db *sql.DB
}

func newMySQLStore(dsn string) (*mysqlStore, error) {
	db, err := sql.Open("mysql", dsn+"?parseTime=true&multiStatements=true")
	if err != nil {
		return nil, fmt.Errorf("hourglass: open mysql: %w", err)
	}
	db.SetMaxOpenConns(16)
	s := &mysqlStore{db: db}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, fmt.Errorf("hourglass: mysql migrate: %w", err)
	}
	return s, nil
}

// partitionCount is the physical partition count of the jobs table.
// MySQL HASH partitioning puts logical shard s in partition s % 32, so
// the 256 logical shards over-partition 32 physical partitions: scan
// pruning is per-partition, and a partition holds 8 logical shards
// (the shard predicate stays in every query).
const partitionCount = 32

const mysqlSchema = `
CREATE TABLE IF NOT EXISTS jobs (
	id          VARCHAR(64) NOT NULL,
	topic       VARCHAR(255) NOT NULL,
	payload     BLOB,
	due         BIGINT NOT NULL,
	shard       INT NOT NULL DEFAULT 0,
	sched_at    BIGINT NOT NULL DEFAULT 0,
	state       VARCHAR(16) NOT NULL DEFAULT 'waiting',
	cron        VARCHAR(64) NOT NULL DEFAULT '',
	attempts    INT NOT NULL DEFAULT 0,
	max_attempts INT NOT NULL DEFAULT 1,
	backoff     INT NOT NULL DEFAULT 30,
	epoch       BIGINT NOT NULL DEFAULT 0,
	acquired_at BIGINT NOT NULL DEFAULT 0,
	terminal_at BIGINT NOT NULL DEFAULT 0,
	cancel_reason TEXT NOT NULL,
	created_at  BIGINT NOT NULL,
	PRIMARY KEY (id, shard),
	INDEX idx_jobs_due_state_shard (shard, state, due)
)
PARTITION BY HASH (shard) PARTITIONS 32;
CREATE TABLE IF NOT EXISTS receipts (
	id       VARCHAR(64) PRIMARY KEY,
	job_id   VARCHAR(64) NOT NULL,
	kind     VARCHAR(16) NOT NULL,
	attempt  INT NOT NULL DEFAULT 1,
	error    TEXT,
	reason   TEXT,
	at       BIGINT NOT NULL,
	state    VARCHAR(16) NOT NULL DEFAULT '',
	INDEX idx_receipts_job (job_id, at)
);
`

func (s *mysqlStore) migrate() error {
	_, err := s.db.Exec(mysqlSchema)
	return err
}

// resetAll clears both tables (integration tests only).
func (s *mysqlStore) resetAll(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, `DELETE FROM receipts`); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, `DELETE FROM jobs`)
	return err
}

const jobCols = `id, topic, payload, due, shard, sched_at, state, cron, attempts, max_attempts, backoff, cancel_reason`

func (s *mysqlStore) Ping(ctx context.Context) error {
	var one int
	return s.db.QueryRowContext(ctx, `SELECT 1`).Scan(&one)
}

func (s *mysqlStore) Insert(ctx context.Context, j *Job) error {
	if j.ID == "" {
		j.ID = newID()
	}
	if j.CreatedAt.IsZero() {
		j.CreatedAt = time.Now()
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO jobs (id, topic, payload, due, shard, sched_at, state, cron, attempts, max_attempts, backoff, cancel_reason, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		j.ID, j.Topic, j.Payload, j.Due.Unix(), j.Shard, j.ScheduledAt.Unix(), string(j.State), j.Cron,
		j.Attempts, j.MaxAttempts, int64(j.Backoff/time.Second), j.CancelReason, j.CreatedAt.Unix())
	return err
}

func scanJobRow(scan func(...any) error) (*Job, error) {
	var j Job
	var state string
	var due, sched, attempts, maxAttempts, backoff, shard int64
	var payload []byte
	if err := scan(&j.ID, &j.Topic, &payload, &due, &shard, &sched, &state, &j.Cron,
		&attempts, &maxAttempts, &backoff, &j.CancelReason); err != nil {
		return nil, err
	}
	j.Payload = payload
	j.Due = time.Unix(due, 0).UTC()
	j.Shard = int(shard)
	if sched != 0 {
		j.ScheduledAt = time.Unix(sched, 0).UTC()
	}
	j.State = State(state)
	j.Attempts, j.MaxAttempts = int(attempts), int(maxAttempts)
	j.Backoff = time.Duration(backoff) * time.Second
	return &j, nil
}

func (s *mysqlStore) Get(ctx context.Context, id string) (*Job, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+jobCols+` FROM jobs WHERE id = ?`, id)
	j, err := scanJobRow(row.Scan)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("hourglass: job %q not found", id)
	}
	return j, err
}

func (s *mysqlStore) Select(ctx context.Context, unit ScanUnit, from, to time.Time, n int) ([]Job, error) {
	q := `SELECT ` + jobCols + ` FROM jobs WHERE state='waiting' AND due >= ? AND due <= ?`
	args := []any{from.Unix(), to.Unix()}
	if unit != scanUnitAll {
		q += ` AND shard = ?`
		args = append(args, unit2shard(unit))
	}
	q += ` ORDER BY due ASC LIMIT ?`
	args = append(args, n)
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Job
	for rows.Next() {
		j, err := scanJobRow(rows.Scan)
		if err != nil {
			return nil, err
		}
		out = append(out, *j)
	}
	return out, rows.Err()
}

// Acquire CAS's waiting -> acquired for these ids under this epoch,
// stamping acquired_at (the lease clock for reclaim).
func (s *mysqlStore) Acquire(ctx context.Context, unit ScanUnit, epoch uint64, ids []string) ([]Job, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	now := time.Now().Unix()
	var won []Job
	for off := 0; off < len(ids); off += sqlChunk {
		end := off + sqlChunk
		if end > len(ids) {
			end = len(ids)
		}
		part := ids[off:end]
		ph := strings.Repeat("?,", len(part))
		q := `UPDATE jobs SET state='acquired', epoch=?, acquired_at=? WHERE state='waiting' AND id IN (` + ph[:len(ph)-1] + `)`
		args := make([]any, 0, len(part)+2)
		args = append(args, epoch, now)
		for _, id := range part {
			args = append(args, id)
		}
		res, err := s.db.ExecContext(ctx, q, args...)
		if err != nil {
			return nil, err
		}
		if n, _ := res.RowsAffected(); n == 0 {
			continue // nothing won in this chunk; do not re-read losers
		}
		// re-read winners
		q2 := `SELECT ` + jobCols + ` FROM jobs WHERE state='acquired' AND id IN (` + ph[:len(ph)-1] + `)`
		rargs := make([]any, 0, len(part))
		for _, id := range part {
			rargs = append(rargs, id)
		}
		rows, err := s.db.QueryContext(ctx, q2, rargs...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			j, err := scanJobRow(rows.Scan)
			if err != nil {
				rows.Close()
				return nil, err
			}
			won = append(won, *j)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}
	return won, nil
}

// Reclaim takes over in-flight rows for a shard: any acquired/fired row
// whose EPOCH predates the new owner's epoch is stale by definition
// (ownership was revoked; the old owner's writes are fenced), and rows
// whose lease (acquired_at) aged past the TTL are stale by age. Both
// requeue to waiting under the NEW epoch — the new owner's first act
// after any takeover. One statement, no coordinator: the database is
// the arbiter (river lesson #11 + epoch fencing).
func (s *mysqlStore) Reclaim(ctx context.Context, unit ScanUnit, epoch uint64, ttl time.Duration) (int, error) {
	cutoff := time.Now().Add(-ttl).Unix()
	q := `UPDATE jobs SET state='waiting', epoch=?, attempts=0, terminal_at=0
	      WHERE state IN ('acquired','fired')
	        AND (epoch < ? OR (acquired_at > 0 AND acquired_at < ?))`
	args := []any{epoch, epoch, cutoff}
	if unit != scanUnitAll {
		q += ` AND shard = ?`
		args = append(args, unit2shard(unit))
	}
	res, err := s.db.ExecContext(ctx, q, args...)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

// transition advances one row along a legal edge (CAS on from-state).
func (s *mysqlStore) transition(ctx context.Context, id string, from, to State) error {
	if !legalEdges[[2]State{from, to}] {
		return fmt.Errorf("hourglass: illegal edge %s -> %s for %s", from, to, id)
	}
	terminal := to == Completed || to == Dead || to == Canceled
	var res sql.Result
	var err error
	if terminal {
		res, err = s.db.ExecContext(ctx,
			`UPDATE jobs SET state=?, terminal_at=? WHERE id=? AND state=?`, string(to), time.Now().Unix(), id, string(from))
	} else {
		res, err = s.db.ExecContext(ctx,
			`UPDATE jobs SET state=? WHERE id=? AND state=?`, string(to), id, string(from))
	}
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return fmt.Errorf("hourglass: transition %s -> %s for %s: row not in %s", from, to, id, from)
	}
	return nil
}

// transitionBatch advances rows along one legal edge, chunked.
func (s *mysqlStore) transitionBatch(ctx context.Context, from, to State, ids []string, at time.Time) error {
	if len(ids) == 0 {
		return nil
	}
	if !legalEdges[[2]State{from, to}] {
		return fmt.Errorf("hourglass: illegal edge %s -> %s (batch of %d)", from, to, len(ids))
	}
	terminal := to == Completed || to == Dead || to == Canceled
	for off := 0; off < len(ids); off += sqlChunk {
		end := off + sqlChunk
		if end > len(ids) {
			end = len(ids)
		}
		part := ids[off:end]
		ph := strings.Repeat("?,", len(part))
		var q string
		var args []any
		if terminal {
			q = `UPDATE jobs SET state=?, terminal_at=? WHERE state=? AND id IN (`
			args = append(args, string(to), at.Unix(), string(from))
		} else {
			q = `UPDATE jobs SET state=? WHERE state=? AND id IN (`
			args = append(args, string(to), string(from))
		}
		for _, id := range part {
			args = append(args, id)
		}
		if _, err := s.db.ExecContext(ctx, q+ph[:len(ph)-1]+`)`, args...); err != nil {
			return err
		}
	}
	return nil
}

// completeWithReceipt flips fired -> completed and writes its receipt
// in ONE transaction (lesson #9: the durable row is the commit point;
// state and receipt must not be split).
func (s *mysqlStore) completeWithReceipt(ctx context.Context, id string, attempt int, errText string, at time.Time) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx,
		`UPDATE jobs SET state='completed', terminal_at=? WHERE id=? AND state='fired'`, at.Unix(), id)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return fmt.Errorf("hourglass: complete %s: row not fired", id)
	}
	_, err = tx.ExecContext(ctx,
		`INSERT INTO receipts (id, job_id, kind, attempt, error, reason, at, state) VALUES (?, ?, 'completed', ?, ?, '', ?, 'completed')`,
		newID(), id, attempt, errText, at.Unix())
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (s *mysqlStore) setDue(ctx context.Context, id string, due time.Time) error {
	_, err := s.db.ExecContext(ctx, `UPDATE jobs SET due=? WHERE id=?`, due.Unix(), id)
	return err
}

func (s *mysqlStore) cancel(ctx context.Context, id, reason string) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE jobs SET state='canceled', cancel_reason=?, terminal_at=? WHERE id=? AND state IN ('waiting','acquired')`,
		reason, time.Now().Unix(), id)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return fmt.Errorf("hourglass: job %s no longer cancelable", id)
	}
	return nil
}

func (s *mysqlStore) minPendingDueIn(ctx context.Context, unit ScanUnit) (time.Time, bool, error) {
	var due sql.NullInt64
	var err error
	if unit == scanUnitAll {
		err = s.db.QueryRowContext(ctx, `SELECT MIN(due) FROM jobs WHERE state='waiting'`).Scan(&due)
	} else {
		err = s.db.QueryRowContext(ctx, `SELECT MIN(due) FROM jobs WHERE state='waiting' AND shard=?`, unit2shard(unit)).Scan(&due)
	}
	if err != nil {
		return time.Time{}, false, err
	}
	if !due.Valid || due.Int64 == 0 {
		return time.Time{}, false, nil
	}
	return time.Unix(due.Int64, 0).UTC(), true, nil
}

func (s *mysqlStore) attemptFor(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	for off := 0; off < len(ids); off += sqlChunk {
		end := off + sqlChunk
		if end > len(ids) {
			end = len(ids)
		}
		part := ids[off:end]
		ph := strings.Repeat("?,", len(part))
		args := make([]any, 0, len(part))
		for _, id := range part {
			args = append(args, id)
		}
		if _, err := s.db.ExecContext(ctx, `UPDATE jobs SET attempts=attempts+1 WHERE state='fired' AND id IN (`+ph[:len(ph)-1]+`)`, args...); err != nil {
			return err
		}
	}
	return nil
}

func (s *mysqlStore) attemptForRollback(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	for off := 0; off < len(ids); off += sqlChunk {
		end := off + sqlChunk
		if end > len(ids) {
			end = len(ids)
		}
		part := ids[off:end]
		ph := strings.Repeat("?,", len(part))
		args := make([]any, 0, len(part))
		for _, id := range part {
			args = append(args, id)
		}
		if _, err := s.db.ExecContext(ctx, `UPDATE jobs SET attempts=attempts-1 WHERE state='waiting' AND id IN (`+ph[:len(ph)-1]+`)`, args...); err != nil {
			return err
		}
	}
	return nil
}

func (s *mysqlStore) getMany(ctx context.Context, ids []string) (map[string]Job, error) {
	out := make(map[string]Job, len(ids))
	for off := 0; off < len(ids); off += sqlChunk {
		end := off + sqlChunk
		if end > len(ids) {
			end = len(ids)
		}
		part := ids[off:end]
		ph := strings.Repeat("?,", len(part))
		args := make([]any, 0, len(part))
		for _, id := range part {
			args = append(args, id)
		}
		rows, err := s.db.QueryContext(ctx, `SELECT `+jobCols+` FROM jobs WHERE id IN (`+ph[:len(ph)-1]+`)`, args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			j, err := scanJobRow(rows.Scan)
			if err != nil {
				rows.Close()
				return nil, err
			}
			out[j.ID] = *j
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (s *mysqlStore) addReceiptSingle(ctx context.Context, jobID, kind, errText, reason, state string, at time.Time) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO receipts (id, job_id, kind, attempt, error, reason, at, state) VALUES (?, ?, ?, 1, ?, ?, ?, ?)`,
		newID(), jobID, kind, errText, reason, at.Unix(), state)
	return err
}

func (s *mysqlStore) addReceiptAttemptErr(ctx context.Context, ids []string, jobs map[string]Job, kind, errText string, at time.Time) error {
	if len(ids) == 0 {
		return nil
	}
	state := kind
	if kind == "retry" {
		state = "waiting"
	}
	const receiptChunk = 100
	for off := 0; off < len(ids); off += receiptChunk {
		end := off + receiptChunk
		if end > len(ids) {
			end = len(ids)
		}
		part := ids[off:end]
		var sb strings.Builder
		sb.WriteString(`INSERT INTO receipts (id, job_id, kind, attempt, error, reason, at, state) VALUES `)
		args := make([]any, 0, len(part)*8)
		for i, id := range part {
			if i > 0 {
				sb.WriteByte(',')
			}
			att := 1
			if j, ok := jobs[id]; ok {
				att = j.Attempts
			}
			sb.WriteString("(?, ?, ?, ?, ?, '', ?, ?)")
			args = append(args, newID(), id, kind, att, errText, at.Unix(), state)
		}
		if _, err := s.db.ExecContext(ctx, sb.String(), args...); err != nil {
			return err
		}
	}
	return nil
}

func (s *mysqlStore) addReceiptAttempt(ctx context.Context, ids []string, jobs map[string]Job, kind string, at time.Time) error {
	return s.addReceiptAttemptErr(ctx, ids, jobs, kind, "", at)
}

func (s *mysqlStore) retryFor(ctx context.Context, id string, now time.Time, backoff time.Duration) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE jobs SET state='waiting', due=? WHERE id=? AND state='fired'`,
		now.Add(backoff).Unix(), id)
	return err
}

// purgeTerminal deletes terminal rows past ttl in bounded batches.
// Partitioned-schema note (DESIGN retention): when a physical partition
// is fully terminal it can be DROPped wholesale — a DDL instead of
// batched DELETEs. Detection: SELECT partitions whose non-terminal rows
// are all older than the retention floor. Not yet implemented: DROP
// changes DDL ownership (needs a lock brief enough for online use);
// tracked as the fast path once measured DELETE pressure justifies it.
func (s *mysqlStore) purgeTerminal(ctx context.Context, now time.Time, ttl time.Duration, limit int) (int, error) {
	cutoff := now.Add(-ttl).Unix()
	rows, err := s.db.QueryContext(ctx,
		`SELECT id FROM jobs WHERE state IN ('completed','dead','canceled') AND terminal_at > 0 AND terminal_at < ? LIMIT ?`,
		cutoff, limit)
	if err != nil {
		return 0, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if len(ids) == 0 {
		return 0, nil
	}
	ph := strings.Repeat("?,", len(ids))
	ph = ph[:len(ph)-1]
	args := make([]any, 0, len(ids))
	for _, id := range ids {
		args = append(args, id)
	}
	if _, err := s.db.ExecContext(ctx, `DELETE FROM receipts WHERE job_id IN (`+ph+`)`, args...); err != nil {
		return 0, err
	}
	res, err := s.db.ExecContext(ctx, `DELETE FROM jobs WHERE id IN (`+ph+`)`, args...)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

func (s *mysqlStore) receipts(ctx context.Context, jobID string) ([]Receipt, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, job_id, kind, attempt, error, reason, at FROM receipts WHERE job_id = ? ORDER BY at ASC, id ASC`, jobID)
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
