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
	workers     int           // handler dispatch concurrency
}

func defaultEngineConfig() engineConfig {
	return engineConfig{
		lookahead:   30 * time.Minute,
		admitLimit:  1000,
		misfireHalo: 10 * time.Minute,
		workers:     8,
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

	// dispatch delivers fired entries to handlers on a worker pool,
	// outside the wheel lock: one slow handler must not stall ticks,
	// admission, or cancel.
	dispatch chan wheelEntry
	wg       sync.WaitGroup
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
	if cfg.workers <= 0 {
		cfg.workers = defaultEngineConfig().workers
	}
	e := &engine{
		store:    s,
		clk:      clk,
		cfg:      cfg,
		epoch:    1,
		handlers: make(map[string]Handler),
		dispatch: make(chan wheelEntry, 1024),
	}
	e.wheel = newWheel(clk, e.epoch)
	e.startWorkers()
	return e
}

// startWorkers launches the handler pool.
func (e *engine) startWorkers() {
	for i := 0; i < e.cfg.workers; i++ {
		e.wg.Add(1)
		go func() {
			defer e.wg.Done()
			for entry := range e.dispatch {
				e.fireEntry(entry)
			}
		}()
	}
}

// Register binds a handler to a topic.
func (e *engine) Register(topic string, h Handler) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.handlers[topic] = h
}

// Submit persists a job and returns its id.
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
	j := &Job{Topic: topic, Payload: payload, Due: c.due, ScheduledAt: c.due, State: Waiting}
	if c.jitter > 0 {
		j.Due = j.Due.Add(drawJitter(c.jitter, time.Now().UnixNano()))
	}
	if err := e.store.Insert(ctx, j); err != nil {
		return "", err
	}
	return j.ID, nil
}

// Every schedules a recurring job from a standard 5-field cron
// expression. The same job row reschedules after each completion
// (completed -> waiting with the next occurrence).
func (e *engine) Every(ctx context.Context, topic, spec string) (string, error) {
	return e.EveryPayload(ctx, topic, spec, nil)
}

// EveryPayload is Every with a payload.
func (e *engine) EveryPayload(ctx context.Context, topic, spec string, payload []byte) (string, error) {
	cs, err := parseCron(spec)
	if err != nil {
		return "", err
	}
	due := cs.nextAfter(e.clk.Now())
	j := &Job{Topic: topic, Payload: payload, Due: due, State: Waiting, Cron: spec}
	if err := e.store.Insert(ctx, j); err != nil {
		return "", err
	}
	return j.ID, nil
}

// maybeReschedule advances a completed cron job to its next occurrence:
// the completed -> waiting edge, one edge, two callers (cron next or a
// future reschedule_at ack).
func (e *engine) maybeReschedule(ctx context.Context, id string) {
	j, err := e.store.Get(ctx, id)
	if err != nil || j.Cron == "" {
		return
	}
	cs, err := parseCron(j.Cron)
	if err != nil {
		return
	}
	next := cs.nextAfter(j.Due)
	_ = e.transition(ctx, id, Completed, Waiting) // reschedule owns this edge
	_ = e.store.(*sqliteStore).setDue(ctx, id, next)
}

// Cancel revokes a job: waiting/acquired rows flip to canceled (a
// receipt — "was planned, was revoked"); admitted-but-unfired wheel
// entries are marked and skipped at fire time. Cancel after fire is
// rejected: there is no cancellation promise past the fire moment.
func (e *engine) Cancel(ctx context.Context, id, reason string) error {
	j, err := e.store.Get(ctx, id)
	if err != nil {
		return err
	}
	switch j.State {
	case Waiting, Acquired:
		// wheel mark first (no-op if not admitted): a late mark is
		// harmless, a missed mark is a delivery
		e.wheel.Cancel(id)
		if err := e.store.(*sqliteStore).cancel(ctx, id, reason); err != nil {
			return err
		}
		return e.store.(*sqliteStore).addReceipt(ctx, id, "canceled", "", reason, "canceled", e.clk.Now())
	case Fired, Completed, Dead, Canceled:
		return fmt.Errorf("hourglass: job %s already %s; cancel only wins before fire", id, j.State)
	}
	return fmt.Errorf("hourglass: job %s unknown state %s", id, j.State)
}

// Get fetches one job by id.
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
// pass picks them up again. Acquired -> waiting is the anti-loss edge
// for a full wheel.
func (e *engine) release(ctx context.Context, ids []string) error {
	for _, id := range ids {
		if err := e.transition(ctx, id, Acquired, Waiting); err != nil {
			return err
		}
	}
	return nil
}

// pump advances the wheel to now and dispatches what fires to the
// worker pool (handlers never run under the wheel lock).
func (e *engine) pump(now time.Time) {
	for _, entry := range e.wheel.Tick(now) {
		e.dispatch <- entry
	}
}

// fireEntry is the dispatch path: CAS to fired, run handler, complete
// or dead-letter, reschedule cron jobs.
func (e *engine) fireEntry(entry wheelEntry) {
	ctx := context.Background()
	now := e.clk.Now()
	if err := e.transition(ctx, entry.id, Acquired, Fired); err != nil {
		return // canceled meanwhile or lost race — skip delivery
	}
	_ = e.store.(*sqliteStore).addReceipt(ctx, entry.id, "fired", "", "", "fired", now)
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
	_ = e.transition(ctx, entry.id, Fired, next)
	kind, errText := "completed", ""
	if next == Dead {
		kind, errText = "dead", herr.Error()
	}
	_ = e.store.(*sqliteStore).addReceipt(ctx, entry.id, kind, errText, "", string(next), e.clk.Now())
	if next == Completed {
		e.maybeReschedule(ctx, entry.id)
	}
}

func (e *engine) handlerFor(topic string) Handler {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.handlers[topic]
}

// transition advances a job's state atomically: UPDATE ... WHERE
// state = from. Illegal edges and lost races fail cleanly.
func (e *engine) transition(ctx context.Context, id string, from, to State) error {
	return e.store.(*sqliteStore).transition(ctx, id, from, to)
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

// Close stops the engine: stop ticking, drain in-flight dispatch,
// release the wheel, wait for handlers.
func (e *engine) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	e.mu.Unlock()

	close(e.dispatch) // workers drain the channel then exit
	e.wg.Wait()
	return e.wheel.Close()
}
