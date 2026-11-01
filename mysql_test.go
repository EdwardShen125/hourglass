package hourglass

import (
	"context"
	"os"
	"testing"
	"time"
)

// MySQL store integration (cluster step a). Gated on TEST_MYSQL_DSN;
// skips gracefully when no database is available (skill convention).
//
//	TEST_MYSQL_DSN='root:hgtest@tcp(127.0.0.1:33061)/hourglass'
func newMySQLTestStore(t *testing.T) *mysqlStore {
	t.Helper()
	dsn := os.Getenv("TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("TEST_MYSQL_DSN not set; mysql integration skipped")
	}
	s, err := newMySQLStore(dsn)
	if err != nil {
		t.Fatalf("newMySQLStore: %v", err)
	}
	t.Cleanup(func() { s.db.Close() })
	if err := s.resetAll(context.Background()); err != nil {
		t.Fatalf("reset: %v", err)
	}
	return s
}

func TestMySQLSchemaAndSubmit(t *testing.T) {
	s := newMySQLTestStore(t)
	ctx := context.Background()
	due := time.Date(2026, 11, 1, 9, 0, 0, 0, time.UTC).Add(time.Minute)

	j := &Job{Topic: "t", Payload: []byte(`{}`), Due: due, ScheduledAt: due, State: Waiting, Shard: 17, Attempts: 0, MaxAttempts: 1, Backoff: time.Second, CreatedAt: time.Now()}
	if err := s.Insert(ctx, j); err != nil {
		t.Fatalf("insert: %v", err)
	}
	got, err := s.Get(ctx, j.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Topic != "t" || got.Shard != 17 || !got.Due.Equal(due) {
		t.Fatalf("roundtrip mismatch: %+v", got)
	}
}

func TestMySQLShardScopedSelectAcquire(t *testing.T) {
	s := newMySQLTestStore(t)
	ctx := context.Background()
	base := time.Date(2026, 11, 1, 9, 0, 0, 0, time.UTC)

	a := &Job{Topic: "t", Due: base, State: Waiting, Shard: 3}
	b := &Job{Topic: "t", Due: base, State: Waiting, Shard: 7}
	s.Insert(ctx, a)
	s.Insert(ctx, b)

	got, err := s.Select(ctx, ScanUnit("3"), base.Add(-time.Hour), base.Add(time.Hour), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].ID != a.ID {
		t.Fatalf("shard-3 select = %v, want exactly [a]", got)
	}

	won, err := s.Acquire(ctx, ScanUnit("3"), 42, []string{a.ID})
	if err != nil {
		t.Fatal(err)
	}
	if len(won) != 1 || won[0].State != Acquired {
		t.Fatalf("acquire = %+v", won)
	}
	// epoch recorded on the row: the ownership credential
	var epoch int64
	s.db.QueryRowContext(ctx, `SELECT epoch FROM jobs WHERE id=?`, a.ID).Scan(&epoch)
	if epoch != 42 {
		t.Fatalf("epoch on row = %d, want 42", epoch)
	}
	// second acquire loses (CAS)
	won2, _ := s.Acquire(ctx, ScanUnit("3"), 43, []string{a.ID})
	if len(won2) != 0 {
		t.Fatalf("double acquire won %d", len(won2))
	}
}

func TestMySQLAcquiredAtDrivesReclaim(t *testing.T) {
	// river-style lease reclaim (lesson #11): rows whose acquired_at
	// aged past the TTL are reclaimable in one statement, without any
	// coordinator notification.
	s := newMySQLTestStore(t)
	ctx := context.Background()
	base := time.Date(2026, 11, 1, 9, 0, 0, 0, time.UTC)

	j := &Job{Topic: "t", Due: base, State: Waiting, Shard: 5}
	s.Insert(ctx, j)
	if _, err := s.Acquire(ctx, ScanUnit("5"), 7, []string{j.ID}); err != nil {
		t.Fatal(err)
	}

	// fresh acquisition: not reclaimable
	if n, err := s.Reclaim(ctx, ScanUnit("5"), 8, time.Minute); err != nil || n != 0 {
		t.Fatalf("reclaim of fresh row: n=%d err=%v, want 0", n, err)
	}

	// age the row past the TTL, then a new epoch reclaims it
	if _, err := s.db.ExecContext(ctx, `UPDATE jobs SET acquired_at = acquired_at - 120 WHERE id=?`, j.ID); err != nil {
		t.Fatal(err)
	}
	n, err := s.Reclaim(ctx, ScanUnit("5"), 9, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("reclaim = %d, want 1", n)
	}
	got, _ := s.Get(ctx, j.ID)
	if got.State != Waiting {
		t.Fatalf("reclaimed row state = %s, want waiting", got.State)
	}
	var epoch int64
	s.db.QueryRowContext(ctx, `SELECT epoch FROM jobs WHERE id=?`, j.ID).Scan(&epoch)
	if epoch != 9 {
		t.Fatalf("reclaim did not stamp new epoch: %d", epoch)
	}
	// and it is admissible again
	js, err := s.Select(ctx, ScanUnit("5"), base.Add(-time.Hour), base.Add(time.Hour), 10)
	if err != nil || len(js) != 1 {
		t.Fatalf("post-reclaim select = %d err=%v", len(js), err)
	}
}

func TestMySQLTransitionAndReceiptsInOneTx(t *testing.T) {
	// lesson #2/#9: terminal transition + receipt share one transaction
	// on MySQL (the publish-seam class of failure cannot split them).
	s := newMySQLTestStore(t)
	ctx := context.Background()
	base := time.Date(2026, 11, 1, 9, 0, 0, 0, time.UTC)

	j := &Job{Topic: "t", Due: base, State: Waiting, Shard: 2, MaxAttempts: 1}
	s.Insert(ctx, j)
	s.Acquire(ctx, ScanUnit("2"), 1, []string{j.ID})
	if err := s.transition(ctx, j.ID, Acquired, Fired); err != nil {
		t.Fatalf("acquired->fired: %v", err)
	}

	if err := s.completeWithReceipt(ctx, j.ID, 1, "fired-at", base); err != nil {
		t.Fatalf("completeWithReceipt: %v", err)
	}
	got, _ := s.Get(ctx, j.ID)
	if got.State != Completed {
		t.Fatalf("state = %s", got.State)
	}
	rs, err := s.receipts(ctx, j.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(rs) == 0 || rs[0].Kind != "completed" {
		t.Fatalf("receipts = %+v", rs)
	}
}

func TestMySQLClusterEndToEnd(t *testing.T) {
	// the b-step Cluster driving a real MySQL store, three shards.
	s := newMySQLTestStore(t)
	// mysqlStore counts writes too (statement batching portability)
	clk := &fakeClock{now: time.Date(2026, 11, 1, 9, 0, 0, 0, time.UTC)}
	c := newCluster(s, clk, engineConfig{lookahead: time.Minute, admitLimit: 50}, []ScanUnit{"3", "7", "11"})
	defer c.Close()
	ctx := context.Background()

	fired := make(chan string, 16)
	c.Register("t", func(_ context.Context, j Job) error {
		fired <- j.ID
		return nil
	})
	base := clk.Now()
	for i, sh := range []int{3, 7, 11} {
		j := &Job{Topic: "t", Due: base.Add(time.Duration(i+1) * time.Second), State: Waiting, Shard: sh}
		if err := s.Insert(ctx, j); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.stepAll(ctx); err != nil {
		t.Fatal(err)
	}
	c.pumpAll(base.Add(30 * time.Second))
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && len(fired) < 3 {
		time.Sleep(10 * time.Millisecond)
	}
	if len(fired) != 3 {
		t.Fatalf("fired %d/3 shards on MySQL", len(fired))
	}
}
