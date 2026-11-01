package hourglass

import (
	"context"
	"os"
	"testing"
	"time"
)

// Shard registry integration (cluster step c): etcd-leased shard
// ownership with epoch fencing. Gated on TEST_ETCD_ADDR.

func newTestRegistry(t *testing.T) *registry {
	t.Helper()
	addr := os.Getenv("TEST_ETCD_ADDR")
	if addr == "" {
		t.Skip("TEST_ETCD_ADDR not set; etcd integration skipped")
	}
	r, err := newRegistry([]string{addr}, 5*time.Second)
	if err != nil {
		t.Fatalf("newRegistry: %v", err)
	}
	t.Cleanup(func() { r.Close() })
	if err := r.reset(context.Background()); err != nil {
		t.Fatalf("reset: %v", err)
	}
	return r
}

func TestRegistryClaimGrantsExclusiveOwnership(t *testing.T) {
	r := newTestRegistry(t)
	ctx := context.Background()

	own, epoch, err := r.Claim(ctx, 17)
	if err != nil {
		t.Fatal(err)
	}
	if !own || epoch == 0 {
		t.Fatalf("claim = own:%v epoch:%d, want own + nonzero epoch", own, epoch)
	}

	// a second claimer (same process, different session) loses
	own2, _, err := r.Claim(ctx, 17)
	if err != nil {
		t.Fatal(err)
	}
	if own2 {
		t.Fatal("second claimer won the same shard; ownership not exclusive")
	}

	// release, then the second claimer wins with a HIGHER epoch
	if err := r.Release(ctx, 17); err != nil {
		t.Fatal(err)
	}
	own3, epoch3, err := r.Claim(ctx, 17)
	if err != nil {
		t.Fatal(err)
	}
	if !own3 || epoch3 <= epoch {
		t.Fatalf("re-claim = own:%v epoch:%d, want own + epoch > %d", own3, epoch3, epoch)
	}
}

func TestRegistryLeaseExpiryFreesShard(t *testing.T) {
	// a crashed owner's lease expires; the shard becomes claimable
	// again with a higher epoch (ungraceful handoff)
	r := newTestRegistry(t)
	ctx := context.Background()

	own, epoch, err := r.Claim(ctx, 5)
	if err != nil || !own {
		t.Fatalf("claim: own=%v err=%v", own, err)
	}

	// simulate the crash: drop the session WITHOUT releasing (lease
	// expires after ttl)
	r.Crash()

	// wait past the lease ttl, then reclaim
	time.Sleep(6 * time.Second)
	own2, epoch2, err := r.Claim(ctx, 5)
	if err != nil {
		t.Fatal(err)
	}
	if !own2 || epoch2 <= epoch {
		t.Fatalf("post-expiry claim = own:%v epoch:%d, want own + epoch > %d", own2, epoch2, epoch)
	}
}

func TestRegistryWatchDeliversOwnershipLoss(t *testing.T) {
	// lesson #12 (Redis PR #12344): ownership loss must be an explicit
	// event, never inferred. The owner learns it lost a shard via watch.
	r := newTestRegistry(t)
	ctx := context.Background()

	own, _, err := r.Claim(ctx, 9)
	if err != nil || !own {
		t.Fatalf("claim: own=%v err=%v", own, err)
	}

	lost := make(chan int, 4)
	cancel := r.WatchLost(ctx, func(shard int) {
		lost <- shard
	})
	defer cancel()

	// force loss: a foreign writer bumps the epoch key directly (the
	// etcd equivalent of the registry revoking ownership)
	if err := r.ForceEpochBump(ctx, 9); err != nil {
		t.Fatal(err)
	}
	select {
	case sh := <-lost:
		if sh != 9 {
			t.Fatalf("watch reported shard %d, want 9", sh)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ownership loss was never delivered via watch")
	}
}

func TestTwoNodeShardFailoverEndToEnd(t *testing.T) {
	// THE load-bearing test: two nodes over one MySQL store; kill the
	// shard's owner; the survivor must reclaim and re-fire the in-flight
	// job — no loss, no permanent orphan (DESIGN four-load-bearing #2).
	mysqlDSN := os.Getenv("TEST_MYSQL_DSN")
	etcdAddr := os.Getenv("TEST_ETCD_ADDR")
	if mysqlDSN == "" || etcdAddr == "" {
		t.Skip("TEST_MYSQL_DSN / TEST_ETCD_ADDR not set; failover e2e skipped")
	}
	ctx := context.Background()

	store, err := newMySQLStore(mysqlDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.db.Close() })
	if err := store.resetAll(ctx); err != nil {
		t.Fatal(err)
	}

	base := time.Now().UTC().Truncate(time.Second)
	// job in shard 4 due in 1s
	j := &Job{Topic: "t", Due: base.Add(time.Second), ScheduledAt: base.Add(time.Second),
		State: Waiting, Shard: 4, MaxAttempts: 1, CreatedAt: time.Now()}
	if err := store.Insert(ctx, j); err != nil {
		t.Fatal(err)
	}

	// node A owns shard 4
	regA, err := newRegistry([]string{etcdAddr}, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer regA.Close()
	own, epoch, err := regA.Claim(ctx, 4)
	if err != nil || !own {
		t.Fatalf("node A claim: own=%v err=%v", own, err)
	}

	// node A acquires the job then "crashes" mid-flight
	won, err := store.Acquire(ctx, ScanUnit("4"), epoch, []string{j.ID})
	if err != nil || len(won) != 1 {
		t.Fatalf("acquire: %v %d", err, len(won))
	}
	regA.Crash()

	// node B: claims the expired shard, reclaims stale rows, reschedules
	regB, err := newRegistry([]string{etcdAddr}, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer regB.Close()
	deadline := time.Now().Add(10 * time.Second)
	var ownB bool
	var epochB uint64
	for time.Now().Before(deadline) {
		ownB, epochB, err = regB.Claim(ctx, 4)
		if err == nil && ownB {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if !ownB {
		t.Fatal("node B never won the expired shard")
	}
	n, err := store.Reclaim(ctx, ScanUnit("4"), epochB, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("reclaim = %d, want 1 (the crashed node's in-flight job)", n)
	}

	// node B's engine re-admits and fires it exactly once
	clk := &realClock{}
	eB := newEngineForShard(store, clk, defaultEngineConfig(), ScanUnit("4"), epochB)
	defer eB.Close()
	fired := make(chan string, 4)
	eB.Register("t", func(_ context.Context, jb Job) error {
		fired <- jb.ID
		return nil
	})
	for i := 0; i < 3 && len(fired) == 0; i++ {
		if err := eB.step(ctx); err != nil {
			t.Fatalf("step: %v", err)
		}
		eB.pump(clk.Now().Add(2 * time.Second))
		select {
		case id := <-fired:
			if id != j.ID {
				t.Fatalf("fired %s, want %s", id, j.ID)
			}
			// exactly once: no second delivery
			select {
			case dup := <-fired:
				t.Fatalf("duplicate delivery: %s", dup)
			case <-time.After(300 * time.Millisecond):
			}
			return
		case <-time.After(time.Second):
		}
	}
	t.Fatal("reclaimed job never fired on node B")
}
