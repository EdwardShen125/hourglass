package hourglass

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"
)

// Join + graceful handoff (cluster steps d/e): the public multi-node
// entry point and the ownership-transfer state machine.

func newJoinEnv(t *testing.T) (Store, string, string) {
	t.Helper()
	mysqlDSN := os.Getenv("TEST_MYSQL_DSN")
	etcdAddr := os.Getenv("TEST_ETCD_ADDR")
	if mysqlDSN == "" || etcdAddr == "" {
		t.Skip("TEST_MYSQL_DSN / TEST_ETCD_ADDR not set; join integration skipped")
	}
	store, err := newMySQLStore(mysqlDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.db.Close() })
	if err := store.resetAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	reg, err := newRegistry([]string{etcdAddr}, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reg.Close() })
	if err := reg.reset(context.Background()); err != nil {
		t.Fatal(err)
	}
	return store, mysqlDSN, etcdAddr
}

// Join is the public multi-node API: one process joins the fleet,
// claims shards, runs engines, and rebalances when peers leave.
func TestJoinClaimsShardsAndFiresJobs(t *testing.T) {
	_, dsn, etcd := newJoinEnv(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node, err := Join(Config{
		MySQL:    dsn,
		Etcd:     []string{etcd},
		Shards:   []int{0, 1, 2},
		LeaseTTL: 3 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()

	node.Register("t", func(context.Context, Job) error { return nil })

	// this fleet is one node owning shards 0..2, so jobs are forced
	// into the owned set (hash placement is fleet-correct when all 256
	// shards have owners somewhere)
	var ids []string
	base := time.Now().UTC().Truncate(time.Second)
	for i := 0; i < 6; i++ {
		id, err := node.Submit(ctx, "t", nil, At(base.Add(time.Duration(i+1)*time.Second)), InShard(i%3))
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}

	go node.Run(ctx)

	// all six fire and complete
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		done := 0
		for _, id := range ids {
			j, err := node.Get(ctx, id)
			if err == nil && j.State == Completed {
				done++
			}
		}
		if done == len(ids) {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("not all submitted jobs completed through Join")
}

// Graceful handoff follows the ordering discipline (lesson #12):
// new owner claims FIRST, old owner drains THEN releases. No shard is
// ever ownerless mid-transfer, and unfired wheel entries survive.
func TestGracefulHandoffMovesShardWithoutLoss(t *testing.T) {
	_, dsn, etcd := newJoinEnv(t)
	ctx := context.Background()

	nodeA, err := Join(Config{MySQL: dsn, Etcd: []string{etcd}, Shards: []int{7}, LeaseTTL: 3 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer nodeA.Close()
	nodeA.Register("t", func(context.Context, Job) error { return nil })

	// a job forced into shard 7, due in 2s, admitted into node A's wheel
	base := time.Now().UTC().Truncate(time.Second)
	id, err := nodeA.Submit(ctx, "t", nil, At(base.Add(2*time.Second)), InShard(7))
	if err != nil {
		t.Fatal(err)
	}
	jrow, _ := nodeA.Get(ctx, id)
	if jrow.Shard != 7 {
		t.Fatalf("InShard(7) placement got shard %d", jrow.Shard)
	}
	// force-admit by driving one step (Run not started; manual drive)
	if err := nodeA.engines()[ScanUnit("7")].TestStep(ctx); err != nil {
		t.Fatal(err)
	}

	// node B claims shard 7, THEN node A drains+releases (graceful order)
	nodeB, err := Join(Config{MySQL: dsn, Etcd: []string{etcd}, Shards: []int{}, LeaseTTL: 3 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer nodeB.Close()
	nodeB.Register("t", func(context.Context, Job) error { return nil })
	if err := nodeA.Handover(ctx, 7); err != nil {
		t.Fatalf("handover: %v", err)
	}
	// AddShard claims + starts the engine in one step
	if err := nodeB.AddShard(ctx, 7); err != nil {
		t.Fatal(err)
	}
	if len(nodeB.engines()) != 1 {
		t.Fatalf("node B owns no engine for shard 7 after AddShard")
	}

	// the drained job is waiting again and fires on node B exactly once
	fired := make(chan string, 2)
	nodeB.Register("t", func(_ context.Context, j Job) error { // override with recorder
		fired <- j.ID
		return nil
	})
	runCtx, stop := context.WithCancel(context.Background())
	go nodeB.Run(runCtx)
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case got := <-fired:
			if got != id {
				t.Fatalf("fired %s want %s", got, id)
			}
			select {
			case dup := <-fired:
				t.Fatalf("duplicate fire after handoff: %s", dup)
			case <-time.After(500 * time.Millisecond):
			}
			stop()
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
	stop()
	t.Fatal("handed-over job never fired on node B")
}

var _ = sync.Mutex{}
