package hourglass

import (
	"context"
	"fmt"
	"strconv"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

// registry is the shard-ownership authority for cluster mode (DESIGN
// "Shard registry"). etcd provides the linearizable truth Redis Cluster
// lacks (lesson #12): ownership records carry a monotonically
// increasing epoch, leases expire on crash, and loss of ownership is
// an explicit watch event — never inferred.
//
// Key layout:
//
//	/hourglass/shards/<n>/owner  = "<nodeID> <epoch>"  (lease-attached)
//	/hourglass/shards/<n>/epoch  = "<epoch>"           (no lease; the
//	    durable generation counter — survives owner crashes)
type registry struct {
	cli    *clientv3.Client
	lease  clientv3.LeaseID
	kactx  context.CancelFunc // cancels the session's keepalive loop
	nodeID string
	ttl    time.Duration
}

const shardPrefix = "/hourglass/shards/"

func newRegistry(endpoints []string, ttl time.Duration) (*registry, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
		DialOptions: []grpc.DialOption{grpc.WithBlock()},
	})
	if err != nil {
		return nil, fmt.Errorf("hourglass: etcd: %w", err)
	}
	r := &registry{
		cli:    cli,
		nodeID: newID()[:16],
		ttl:    ttl,
	}
	if err := r.newSession(); err != nil {
		cli.Close()
		return nil, err
	}
	return r, nil
}

// newSession grants a fresh lease and starts its keepalive under a
// session context; Crash cancels that context so the lease truly dies
// with the node (a client-side keepalive goroutine holding a live
// context would otherwise keep a "crashed" node's lease alive forever).
func (r *registry) newSession() error {
	if r.kactx != nil {
		r.kactx()
	}
	sctx, cancel := context.WithCancel(context.Background())
	lease, err := r.cli.Grant(sctx, int64(r.ttl.Seconds()))
	if err != nil {
		cancel()
		return fmt.Errorf("hourglass: etcd lease: %w", err)
	}
	kach, err := r.cli.KeepAlive(sctx, lease.ID)
	if err != nil {
		cancel()
		r.cli.Revoke(context.Background(), lease.ID)
		return fmt.Errorf("hourglass: etcd keepalive: %w", err)
	}
	go func() {
		for range kach { // drain responses
		}
	}()
	r.kactx = cancel
	r.lease = lease.ID
	return nil
}

// Claim tries to take ownership of one shard. Returns (owned, epoch).
// The epoch is the durable generation from /epoch, bumped on every
// ownership change — new owners always observe a strictly larger epoch
// than any previous owner, which is what fences zombie writes.
func (r *registry) Claim(ctx context.Context, shard int) (bool, uint64, error) {
	ownerKey := fmt.Sprintf("%s%d/owner", shardPrefix, shard)
	epochKey := fmt.Sprintf("%s%d/epoch", shardPrefix, shard)

	// transaction: owner key must NOT exist to claim
	resp, err := r.cli.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(ownerKey), "=", 0)).
		Then(clientv3.OpPut(ownerKey, r.nodeID, clientv3.WithLease(r.lease))).
		Else(clientv3.OpGet(ownerKey)).
		Commit()
	if err != nil {
		return false, 0, fmt.Errorf("hourglass: claim: %w", err)
	}
	if !resp.Succeeded {
		return false, 0, nil // someone else owns it
	}

	// bump the durable epoch for this ownership change (atomic putif
	// revision ensures monotonicity even against concurrent bumpers)
	for {
		get, err := r.cli.Get(ctx, epochKey)
		if err != nil {
			return false, 0, err
		}
		var next uint64 = 1
		var modRev int64 = 0
		if len(get.Kvs) > 0 {
			cur, _ := strconv.ParseUint(string(get.Kvs[0].Value), 10, 64)
			next = cur + 1
			modRev = get.Kvs[0].ModRevision
		}
		tresp, err := r.cli.Txn(ctx).
			If(clientv3.Compare(clientv3.ModRevision(epochKey), "=", modRev)).
			Then(clientv3.OpPut(epochKey, strconv.FormatUint(next, 10))).
			Commit()
		if err != nil {
			return false, 0, err
		}
		if tresp.Succeeded {
			return true, next, nil
		}
		// raced on the bump: retry with the fresh value
	}
}

// Release drops ownership gracefully (handoff step: drain first).
func (r *registry) Release(ctx context.Context, shard int) error {
	ownerKey := fmt.Sprintf("%s%d/owner", shardPrefix, shard)
	_, err := r.cli.Delete(ctx, ownerKey)
	return err
}

// Crash simulates a hard kill: the keepalive loop is abandoned WITHOUT
// revoking (revoke would be immediate — a crash is not). The lease
// expires after ttl on its own, which is what the failover path must
// actually handle. A fresh session is prepared for the caller to act
// as a restarted node.
func (r *registry) Crash() {
	if r.kactx != nil {
		r.kactx() // keepalive loop stops; lease expires ttl later
	}
	r.lease = 0
	_ = r.newSession()
}

// WatchLost delivers shard numbers whose ownership record disappeared
// (revoked, released, or lease-expired) — the explicit ownership-loss
// event (lesson #12: never infer loss).
func (r *registry) WatchLost(ctx context.Context, fn func(shard int)) (cancel func()) {
	wctx, ccancel := context.WithCancel(ctx)
	wch := r.cli.Watch(wctx, shardPrefix, clientv3.WithPrefix(), clientv3.WithFilterPut())
	go func() {
		for resp := range wch {
			for _, ev := range resp.Events {
				if ev.Type != clientv3.EventTypeDelete {
					continue
				}
				var shard int
				if n, err := fmt.Sscanf(string(ev.Kv.Key), shardPrefix+"%d/owner", &shard); err == nil && n == 1 {
					fn(shard)
				}
			}
		}
	}()
	return ccancel
}

// ForceEpochBump rewrites the owner key from outside (test hook: the
// registry-side revocation; the owner learns via WatchLost).
func (r *registry) ForceEpochBump(ctx context.Context, shard int) error {
	ownerKey := fmt.Sprintf("%s%d/owner", shardPrefix, shard)
	epochKey := fmt.Sprintf("%s%d/epoch", shardPrefix, shard)
	_, err := r.cli.Txn(ctx).
		Then(
			clientv3.OpDelete(ownerKey),
			clientv3.OpPut(epochKey, strconv.FormatUint(uint64(time.Now().UnixNano()), 10)),
		).
		Commit()
	return err
}

// reset wipes the shard namespace (integration tests only).
func (r *registry) reset(ctx context.Context) error {
	_, err := r.cli.Delete(ctx, shardPrefix, clientv3.WithPrefix())
	return err
}

func (r *registry) Close() error {
	return r.cli.Close()
}
