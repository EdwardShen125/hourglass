package hourglass

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

// Real partitioned tables (the "partitioned scans" the README promises).
// MySQL partitioning rules that shape the schema:
//   - the partition key must be part of EVERY unique key, so the PK is
//     (id, shard) — uniqueness is still per-job because shard is a
//     function of id (fnv-1a), never caller-chosen independently.
//   - HASH(shard) partitions give each logical shard its own physical
//     subtree: scans for shard 7 touch partition p7 only, and terminal
//     partitions can be DROPped wholesale (retention design).

// partOf maps a logical shard (0..DefaultShards-1) to its physical
// partition (0..partitionCount-1). Matches MySQL's HASH semantics:
// partition = shard % partitionCount for integer HASH partitioning.
func partOf(shard int) int {
	if shard < 0 {
		return 0
	}
	return shard % partitionCount
}

// TestPartitionedSchemaAndPruning (gated on TEST_MYSQL_DSN):
//  1. the jobs table is created PARTITION BY HASH(shard)
//  2. EXPLAIN of a shard-filtered scan shows partition pruning (a
//     single partition, not all 32)
func TestPartitionedSchemaAndPruning(t *testing.T) {
	dsn := os.Getenv("TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("TEST_MYSQL_DSN not set; mysql integration skipped")
	}
	s, err := newMySQLStore(dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.db.Close() })
	ctx := context.Background()
	if err := s.resetAll(ctx); err != nil {
		t.Fatal(err)
	}

	// table exists and is partitioned
	var createSQL string
	if err := s.db.QueryRowContext(ctx, `SHOW CREATE TABLE jobs`).Scan(new(string), &createSQL); err != nil {
		t.Fatalf("show create: %v", err)
	}
	if !contains(createSQL, "PARTITION BY HASH") {
		t.Fatalf("jobs table is not partitioned:\n%s", createSQL)
	}
	var parts int
	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM information_schema.PARTITIONS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='jobs' AND PARTITION_NAME IS NOT NULL`).Scan(&parts); err != nil {
		t.Fatal(err)
	}
	if parts != partitionCount {
		t.Fatalf("jobs has %d partitions, want %d", parts, partitionCount)
	}

	// EXPLAIN: a shard-scoped scan must prune to ONE partition
	for shard := 0; shard < DefaultShards; shard += 37 { // sample shards 0,37,74,...221
		rows, err := s.db.QueryContext(ctx,
			`EXPLAIN SELECT id FROM jobs WHERE state='waiting' AND due >= ? AND due <= ? AND shard = ?`,
			time.Now().Add(-time.Hour).Unix(), time.Now().Add(time.Hour).Unix(), shard)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var partitions string
		scanned := false
		for rows.Next() {
			var id, sel, tbl, part, nRows, rRows, filtered, extra string
			var nRowsN, rRowsN interface{}
			_ = nRowsN
			_ = rRowsN
			if err := rows.Scan(&id, &sel, &tbl, &part, &nRows, &rRows, &filtered, &extra); err != nil {
				// EXPLAIN may return 8 or 12 columns depending on version
				continue
			}
			scanned = true
			partitions = part
		}
		if !scanned {
			t.Fatalf("EXPLAIN returned no rows for shard %d", shard)
		}
		want := fmt.Sprintf("p%d", partOf(shard))
		if partitions != want {
			t.Errorf("shard %d: EXPLAIN partitions=%q, want pruned single partition %q", shard, partitions, want)
		}
	}
}

// TestPartitionIsolation: rows for different shards land in different
// partitions (information_schema.PARTITIONS TABLE_ROWS direction), and
// the explicit-partition scan form works.
func TestPartitionIsolation(t *testing.T) {
	dsn := os.Getenv("TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("TEST_MYSQL_DSN not set; mysql integration skipped")
	}
	s, err := newMySQLStore(dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.db.Close() })
	ctx := context.Background()
	if err := s.resetAll(ctx); err != nil {
		t.Fatal(err)
	}

	// rows in shards 5 and 6 (both partition p5/p6 — distinct when
	// partitionCount > 6)
	for _, sh := range []int{5, 6} {
		j := &Job{Topic: "t", Due: time.Now().Add(time.Minute), State: Waiting, Shard: sh, CreatedAt: time.Now()}
		if err := s.Insert(ctx, j); err != nil {
			t.Fatal(err)
		}
	}

	// explicit-partition scan: SELECT ... PARTITION (pN) — the form the
	// sharded scan driver will use in cluster mode
	n5 := countInPartition(ctx, t, s, 5)
	n6 := countInPartition(ctx, t, s, 6)
	if n5 != 1 || n6 != 1 {
		t.Fatalf("partition isolation broken: p5=%d p6=%d, want 1/1", n5, n6)
	}
	// shard 38 also maps to p6: explicit scan of p6 finds both shard-6
	// and shard-38 rows, so the shard column filter is still required —
	// pruning reduces IO, the shard predicate keeps semantics
	j38 := &Job{Topic: "t", Due: time.Now().Add(time.Minute), State: Waiting, Shard: 38, CreatedAt: time.Now()}
	s.Insert(ctx, j38)
	if got := countInPartition(ctx, t, s, 6); got != 2 {
		t.Fatalf("p6 rows = %d, want 2 (shards 6 and 38 share the partition)", got)
	}
}

func countInPartition(ctx context.Context, t *testing.T, s *mysqlStore, part int) int64 {
	t.Helper()
	var n int64
	q := fmt.Sprintf(`SELECT COUNT(*) FROM jobs PARTITION (p%d)`, part)
	if err := s.db.QueryRowContext(ctx, q).Scan(&n); err != nil {
		t.Fatalf("count partition p%d: %v", part, err)
	}
	return n
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(sub) == 0 || indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
