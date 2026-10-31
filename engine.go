package hourglass

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Handler runs one job's business logic. Handlers must be idempotent:
// delivery is at-least-once.
type Handler func(ctx context.Context, j Job) error

// engineConfig bounds the admission loop. The lookahead window is a
// memory budget: wheel memory ≈ admission rate × lookahead × bytes per
// timer (DESIGN.md).
type engineConfig struct {
	lookahead   time.Duration
	admitLimit  int
	misfireHalo time.Duration // how far back the window floor reaches
}

func defaultEngineConfig() engineConfig {
	return engineConfig{
		lookahead:   30 * time.Minute,
		admitLimit:  1000,
		misfireHalo: 10 * time.Minute,
	}
}

// engine ties the store, the admission loop, and the wheel together for
// embedded mode. One engine owns one scan unit.
type engine struct {
	store    Store
	clk      clock
	cfg      engineConfig
	wheel    *wheel
	epoch    uint64
	mu       sync.Mutex
	handlers map[string]Handler
	closed   bool
}

func newEngine(s Store, clk clock, cfg engineConfig) *engine {
	if cfg.lookahead <= 0 {
		cfg.lookahead = defaultEngineConfig().lookahead
	}
	if cfg.admitLimit <= 0 {
		cfg.admitLimit = defaultEngineConfig().admitLimit
	}
	if cfg.misfireHalo <= 0 {
		cfg.misfireHalo = defaultEngineConfig().misfireHalo
	}
	e := &engine{
		store:    s,
		clk:      clk,
		cfg:      cfg,
		epoch:    1,
		handlers: make(map[string]Handler),
	}
	e.wheel = newWheel(clk, e.epoch, e.fireEntry)
	return e
}

// Register binds a handler to a topic.
func (e *engine) Register(topic string, h Handler) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.handlers[topic] = h
}

// Submit and Get are exposed on the engine for tests; the public Handle
// delegates to them.
func (e *engine) Submit(ctx context.Context, topic string, payload []byte, opts ...SubmitOption) (string, error) {
	var c submitConfig
	for _, opt := range opts {
		opt(&c)
	}
	if topic == "" {
		return "", ErrEmptyTopic
	}
	if !c.hasDue {
		return "", ErrMissingDue
	}
	j := &Job{Topic: topic, Payload: payload, Due: c.due, State: Waiting}
	if err := e.store.Insert(ctx, j); err != nil {
		return "", err
	}
	return j.ID, nil
}

func (e *engine) Get(ctx context.Context, id string) (*Job, error) { return e.store.Get(ctx, id) }

// step runs one admission tick: select the window, CAS-acquire, hand
// winners to the wheel. Wheel overflow releases the surplus back to
// waiting — nothing is lost.
func (e *engine) step(ctx context.Context) error {
	now := e.clk.Now()
	from := now.Add(-e.cfg.misfireHalo)
	to := now.Add(e.cfg.lookahead)

	cands, err := e.store.Select(ctx, scanUnitAll, from, to, e.cfg.admitLimit)
	if err != nil {
		return fmt.Errorf("admission select: %w", err)
	}
	if len(cands) == 0 {
		return nil
	}
	ids := make([]string, len(cands))
	for i, c := range cands {
		ids[i] = c.ID
	}
	won, err := e.store.Acquire(ctx, scanUnitAll, e.epoch, ids)
	if err != nil {
		return fmt.Errorf("admission acquire: %w", err)
	}

	var surplus []string
	for i := range won {
		entry := wheelEntry{id: won[i].ID, due: won[i].Due, epoch: e.epoch}
		if err := e.wheel.Offer(entry); err != nil {
			surplus = append(surplus, won[i].ID)
		}
	}
	return e.release(ctx, surplus)
}

// release returns wheel-rejected ids to waiting so the next admission
// pass picks them up again.
func (e *engine) release(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	// v1 minimal: direct release. TODO: store.Release(ctx, ids).
	for _, id := range ids {
		j, err := e.store.Get(ctx, id)
		if err != nil {
			return err
		}
		j.State = Waiting
		if err := e.store.(*sqliteStore).setState(ctx, id, Waiting); err != nil {
			return err
		}
		_ = j
	}
	return nil
}

// pump advances the wheel to now and runs what fires.
func (e *engine) pump(now time.Time) { e.wheel.Tick(now) }

// fireEntry is the wheel's fire callback: state to fired, run handler,
// complete or dead-letter.
func (e *engine) fireEntry(entry wheelEntry, at time.Time) {
	ctx := context.Background()
	if err := e.transition(ctx, entry.id, Fired); err != nil {
		return // job vanished (canceled meanwhile) — skip
	}
	j, err := e.store.Get(ctx, entry.id)
	if err != nil {
		return
	}
	h := e.handlerFor(j.Topic)
	var herr error
	if h == nil {
		herr = fmt.Errorf("hourglass: no handler for topic %q", j.Topic)
	} else {
		herr = h(ctx, *j)
	}
	next := Completed
	if herr != nil {
		next = Dead // v1: single attempt; retry policy is a v1.0 item
	}
	_ = e.transition(ctx, entry.id, next)
}

func (e *engine) handlerFor(topic string) Handler {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.handlers[topic]
}

// transition advances a job's state if the row is still in from-state.
// v1 minimal: read-modify-write on the single-writer SQLite store.
func (e *engine) transition(ctx context.Context, id string, to State) error {
	j, err := e.store.Get(ctx, id)
	if err != nil {
		return err
	}
	j.State = to
	return e.store.(*sqliteStore).setState(ctx, id, to)
}

// Run drives admission and wheel ticks on a one-second cadence until
// ctx is done.
func (e *engine) Run(ctx context.Context) {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-tick.C:
			e.pump(now)
			if err := e.step(ctx); err != nil {
				// admission errors are survivable: next tick retries
				continue
			}
		}
	}
}

// Close stops the engine. Graceful drain is a later cycle.
func (e *engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.closed = true
	return e.wheel.Close()
}
