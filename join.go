package hourglass

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Config configures a cluster node.
type Config struct {
	MySQL    string        // mysql dsn, e.g. "user:pass@tcp(host:3306)/hourglass"
	Etcd     []string      // etcd endpoints
	Shards   []int         // shards this node will try to own (claim races decide)
	LeaseTTL time.Duration // etcd lease ttl (default 15s)
	Engine   engineConfig  // optional engine tuning (zero values take defaults)
}

// Node is one process's membership in the scheduling fleet: the store,
// the shard registry, and one engine per owned shard. Adding a process
// adds shard ownership — that is the scale story.
type Node struct {
	store Store
	reg   *registry
	cfg   Config
	clus  *Cluster
	mu    sync.Mutex
}

// Join connects to MySQL and etcd, claims the configured shards (races
// decide ownership; unclaimed shards here are simply owned by peers),
// and returns a ready node. Call Run to drive scheduling.
func Join(cfg Config) (*Node, error) {
	if cfg.LeaseTTL <= 0 {
		cfg.LeaseTTL = 15 * time.Second
	}
	store, err := newMySQLStore(cfg.MySQL)
	if err != nil {
		return nil, err
	}
	reg, err := newRegistry(cfg.Etcd, cfg.LeaseTTL)
	if err != nil {
		store.db.Close()
		return nil, err
	}
	engCfg := cfg.Engine
	if engCfg.lookahead <= 0 {
		engCfg = defaultEngineConfig()
	}
	clus := newCluster(store, realClock{}, engCfg, nil)
	n := &Node{store: store, reg: reg, cfg: cfg, clus: clus}
	for _, sh := range cfg.Shards {
		if err := n.AddShard(context.Background(), sh); err != nil {
			n.Close()
			return nil, err
		}
	}
	return n, nil
}

// AddShard claims one shard and starts its engine if the claim wins.
// A lost claim is not an error (a peer owns it).
func (n *Node) AddShard(ctx context.Context, shard int) error {
	own, epoch, err := n.reg.Claim(ctx, shard)
	if err != nil {
		return err
	}
	if !own {
		return nil
	}
	n.clus.addShardEpoch(ScanUnit(fmt.Sprint(shard)), epoch)
	n.clus.reclaimShard(ctx, ScanUnit(fmt.Sprint(shard)), epoch)
	return nil
}

// Handover performs the graceful handoff ordering (lesson #12):
// the CALLER ensures the new owner is ready first; this node then stops
// admitting, drains its wheel (unfired entries return to waiting), and
// releases the shard. No in-flight job is lost; the new owner's misfire
// rescan re-admits what was drained.
func (n *Node) Handover(ctx context.Context, shard int) error {
	unit := ScanUnit(fmt.Sprint(shard))
	if err := n.clus.DropShard(unit); err != nil {
		return err
	}
	return n.reg.Release(ctx, shard)
}

// engines exposes the per-shard engines (tests).
func (n *Node) engines() map[ScanUnit]*engine {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make(map[ScanUnit]*engine)
	for k, v := range n.clus.engines {
		out[k] = v
	}
	return out
}

// Register binds a handler on every owned engine (and engines added later).
func (n *Node) Register(topic string, h Handler) { n.clus.Register(topic, h) }

// Submit persists a job; shard placement is hash(id) — any node can
// submit, any owner scans.
func (n *Node) Submit(ctx context.Context, topic string, payload []byte, opts ...SubmitOption) (string, error) {
	return n.clus.submit(ctx, topic, payload, opts...)
}

// Get fetches one job by id.
func (n *Node) Get(ctx context.Context, id string) (*Job, error) { return n.store.Get(ctx, id) }

// Cancel revokes a job before fire (any node).
func (n *Node) Cancel(ctx context.Context, id, reason string) error {
	return cancelInStore(ctx, n.store, id, reason)
}

// Run drives all owned shard engines until ctx is done.
func (n *Node) Run(ctx context.Context) { n.clus.Run(ctx) }

// Close hands nothing over (use Handover per shard for graceful exit)
// and releases everything.
func (n *Node) Close() error {
	err1 := n.clus.Close()
	err2 := n.reg.Close()
	if n.store != nil {
		if closer, ok := n.store.(interface{ Close() error }); ok {
			_ = closer.Close()
		}
	}
	if err1 != nil {
		return err1
	}
	return err2
}

// cancelInStore cancels without a local wheel: the owning node's fire
// path re-validates state before delivery, so a cross-node cancel only
// needs the row flip + receipt.
func cancelInStore(ctx context.Context, s Store, id, reason string) error {
	j, err := s.Get(ctx, id)
	if err != nil {
		return err
	}
	switch j.State {
	case Waiting, Acquired:
		if err := s.cancel(ctx, id, reason); err != nil {
			return err
		}
		return s.addReceiptSingle(ctx, id, "canceled", "", reason, "canceled", time.Now())
	case Fired, Completed, Dead, Canceled:
		return fmt.Errorf("hourglass: job %s already %s; cancel only wins before fire", id, j.State)
	}
	return fmt.Errorf("hourglass: job %s unknown state %s", id, j.State)
}
