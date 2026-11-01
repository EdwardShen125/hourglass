package hourglass

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Cluster drives one engine per owned shard over one store. This is the
// scheduling-plane scale-out unit of cluster mode: adding a process
// adds shard ownership, and each shard's admission loop and wheel run
// independently (share-nothing between shards).
//
// Ownership (which process holds which shards) is the registry's job
// (etcd leases + epoch fencing, DESIGN.md "Shard registry"); Cluster is
// the per-process executor of whatever ownership it is handed.
type Cluster struct {
	store   Store
	clk     clock
	cfg     engineConfig
	epoch   uint64
	mu      sync.Mutex
	engines map[ScanUnit]*engine
	closed  bool
}

// NewCluster creates a cluster executor for the given shards under one
// epoch. All engines share the store and the handler registry.
func newCluster(s Store, clk clock, cfg engineConfig, shards []ScanUnit) *Cluster {
	c := &Cluster{
		store:   s,
		clk:     clk,
		cfg:     cfg,
		epoch:   1,
		engines: make(map[ScanUnit]*engine, len(shards)),
	}
	for _, sh := range shards {
		c.addShard(sh)
	}
	return c
}

// addShard starts an engine for one shard (idempotent).
func (c *Cluster) addShard(sh ScanUnit) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.engines[sh]; ok {
		return
	}
	c.engines[sh] = newEngineForShard(c.store, c.clk, c.cfg, sh, c.epoch)
}

// DropShard stops one shard's engine (graceful handoff step 1: the
// registry revokes ownership, the old owner drains).
func (c *Cluster) DropShard(sh ScanUnit) error {
	c.mu.Lock()
	e, ok := c.engines[sh]
	delete(c.engines, sh)
	c.mu.Unlock()
	if !ok {
		return nil
	}
	// drain: unfired wheel entries go back to waiting so the new owner's
	// misfire rescan re-admits them
	for _, id := range e.wheel.Drain() {
		_ = e.transition(context.Background(), id, Acquired, Waiting)
	}
	return e.Close()
}

// engineFor exposes one shard's engine (tests).
func (c *Cluster) engineFor(sh ScanUnit) *engine {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.engines[sh]
}

// addShardEpoch starts an engine for one shard under the given epoch.
func (c *Cluster) addShardEpoch(sh ScanUnit, epoch uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.engines[sh]; ok {
		return
	}
	c.engines[sh] = newEngineForShard(c.store, c.clk, c.cfg, sh, epoch)
}

// reclaimShard requeues stale in-flight rows under the new epoch — the
// first act of a new owner after claim (graceful or failover).
func (c *Cluster) reclaimShard(ctx context.Context, sh ScanUnit, epoch uint64) {
	if ext, ok := c.store.(StoreExt); ok {
		_, _ = ext.Reclaim(ctx, sh, epoch, 2*time.Minute)
	}
}

// submit persists a job with hash shard placement (any node may submit).
func (c *Cluster) submit(ctx context.Context, topic string, payload []byte, opts ...SubmitOption) (string, error) {
	var cfg submitConfig
	for _, opt := range opts {
		opt(&cfg)
	}
	if topic == "" {
		return "", ErrEmptyTopic
	}
	if !cfg.hasDue {
		return "", ErrMissingDue
	}
	j := &Job{Topic: topic, Payload: payload, Due: cfg.due, ScheduledAt: cfg.due, State: Waiting,
		Attempts: 0, MaxAttempts: max(cfg.maxAttempts, 1), Backoff: cfg.backoff, CreatedAt: time.Now()}
	if cfg.jitter > 0 {
		j.Due = j.Due.Add(drawJitter(cfg.jitter, time.Now().UnixNano()))
	}
	if j.Backoff == 0 {
		j.Backoff = 30 * time.Second
	}
	j.ID = newID()
	j.Shard = ShardFor(j.ID)
	if cfg.shardSet {
		j.Shard = cfg.shard
	}
	if err := c.store.Insert(ctx, j); err != nil {
		return "", err
	}
	return j.ID, nil
}

// Register binds a handler on every shard engine.
func (c *Cluster) Register(topic string, h Handler) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, e := range c.engines {
		e.Register(topic, h)
	}
}

// stepAll runs one admission tick on every owned shard.
func (c *Cluster) stepAll(ctx context.Context) error {
	c.mu.Lock()
	engines := make([]*engine, 0, len(c.engines))
	for _, e := range c.engines {
		engines = append(engines, e)
	}
	c.mu.Unlock()
	var firstErr error
	for _, e := range engines {
		if err := e.step(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// pumpAll advances every shard's wheel to now.
func (c *Cluster) pumpAll(now time.Time) {
	c.mu.Lock()
	engines := make([]*engine, 0, len(c.engines))
	for _, e := range c.engines {
		engines = append(engines, e)
	}
	c.mu.Unlock()
	for _, e := range engines {
		e.pump(now)
	}
}

// Run drives all shards on a one-second cadence until ctx is done.
func (c *Cluster) Run(ctx context.Context) {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-tick.C:
			c.pumpAll(now)
			_ = c.stepAll(ctx)
		}
	}
}

// Close stops every shard engine (drains dispatch, waits for handlers).
func (c *Cluster) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	engines := make([]*engine, 0, len(c.engines))
	for sh, e := range c.engines {
		engines = append(engines, e)
		delete(c.engines, sh)
	}
	c.mu.Unlock()
	var err error
	for _, e := range engines {
		if cerr := e.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("hourglass: cluster close: %w", cerr)
		}
	}
	return err
}
