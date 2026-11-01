package hourglass

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Handler runs one job's business logic. Handlers must be idempotent:
// delivery is at-least-once.
type Handler func(ctx context.Context, j Job) error

// engineConfig bounds the admission loop. The lookahead window is a
// memory budget: wheel memory ≈ admission rate × lookahead × bytes per
// timer (DESIGN.md).
type engineConfig struct {
	lookahead    time.Duration
	admitLimit   int
	misfireHalo  time.Duration // how far back the window floor reaches
	workers      int           // handler dispatch concurrency
	retention    time.Duration // terminal-row TTL (default 7d)
	dispatchSize int           // dispatch channel buffer (default 1024)
}

// quietUntil accessors: the watermark is written by Submit/step from
// different goroutines; guard it with the engine mutex.
func (e *engine) setQuietUntil(t time.Time) {
	e.mu.Lock()
	e.quietUntil = t
	e.mu.Unlock()
}

func (e *engine) quietUntilNow() time.Time {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.quietUntil
}

func dispatchSizeOrDefault(n int) int {
	if n <= 0 {
		return 1024
	}
	return n
}

func defaultEngineConfig() engineConfig {
	return engineConfig{
		lookahead:   30 * time.Minute,
		admitLimit:  1000,
		misfireHalo: 10 * time.Minute,
		workers:     8,
	}
}

// engine ties the store, the admission loop, and the wheel together.
// One engine owns exactly one scan unit (shard); a cluster runs one
// engine per owned shard.
type engine struct {
	store    Store
	clk      clock
	cfg      engineConfig
	unit     ScanUnit // scan unit this engine owns ("" = all, embedded)
	wheel    *wheel
	epoch    uint64
	mu       sync.Mutex
	handlers map[string]Handler
	closed   bool

	// quietUntil: admission sleeps until this moment when the store has
	// nothing admissible (quiescence watermark).
	quietUntil time.Time

	// exec: remote executor target (cluster execution face). nil in
	// embedded handler mode.
	exec *executorTarget

	// dispatch delivers fired entries to handlers on a worker pool,
	// outside the wheel lock: one slow handler must not stall ticks,
	// admission, or cancel.
	dispatch chan wheelTask
	wg       sync.WaitGroup
	done     chan struct{} // closed by Close: stops pump sends and Run
}

func newEngine(s Store, clk clock, cfg engineConfig) *engine {
	return newEngineForShard(s, clk, cfg, scanUnitAll, 1)
}

// newEngineForShard binds an engine to one shard under the given epoch.
func newEngineForShard(s Store, clk clock, cfg engineConfig, unit ScanUnit, epoch uint64) *engine {
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
	if cfg.retention <= 0 {
		cfg.retention = 7 * 24 * time.Hour
	}
	e := &engine{
		store:    s,
		clk:      clk,
		cfg:      cfg,
		unit:     unit,
		epoch:    epoch,
		handlers: make(map[string]Handler),
		dispatch: make(chan wheelTask, dispatchSizeOrDefault(cfg.dispatchSize)),
		done:     make(chan struct{}),
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
			for task := range e.dispatch {
				e.fireEntry(task)
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
	j := &Job{Topic: topic, Payload: payload, Due: c.due, ScheduledAt: c.due, State: Waiting,
		Attempts: 0, MaxAttempts: max(c.maxAttempts, 1), Backoff: c.backoff}
	j.Shard = ShardFor(j.ID)
	if c.jitter > 0 {
		j.Due = j.Due.Add(drawJitter(c.jitter, time.Now().UnixNano()))
	}
	if j.Backoff == 0 {
		j.Backoff = 30 * time.Second
	}
	if err := e.store.Insert(ctx, j); err != nil {
		return "", err
	}
	// new work exists: wake admission (the quiescence watermark must
	// never hide a fresh submit)
	e.setQuietUntil(time.Time{})
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
	if due.IsZero() {
		return "", fmt.Errorf("hourglass: cron %q has no occurrence within the search horizon", spec)
	}
	j := &Job{Topic: topic, Payload: payload, Due: due, State: Waiting, Cron: spec}
	if err := e.store.Insert(ctx, j); err != nil {
		return "", err
	}
	e.setQuietUntil(time.Time{})
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
	if next.IsZero() {
		return // no further occurrence within the horizon: recurrence ends
	}
	_ = e.transition(ctx, id, Completed, Waiting) // reschedule owns this edge
	_ = e.store.setDue(ctx, id, next)
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
		if err := e.store.cancel(ctx, id, reason); err != nil {
			return err
		}
		return e.store.addReceiptSingle(ctx, id, "canceled", "", reason, "canceled", e.clk.Now())
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
	if e.quietUntilNow().After(now) {
		return nil // quiescence watermark: nothing can be due yet
	}
	from := now.Add(-e.cfg.misfireHalo)
	to := now.Add(e.cfg.lookahead)

	cands, err := e.store.Select(ctx, e.unit, from, to, e.cfg.admitLimit)
	if err != nil {
		return fmt.Errorf("admission select: %w", err)
	}
	if len(cands) == 0 {
		// quiescence: find the earliest pending due and sleep admission
		// until it enters the window; one read per quiet period. The
		// wake-up is capped at half the misfire halo: any due that
		// crosses the lookahead while admission sleeps is still inside
		// the scan floor when we wake — a job can never fall below the
		// horizon and get lost (review finding #1).
		minDue, ok, err := e.store.minPendingDueIn(ctx, e.unit)
		if err == nil && ok && minDue.After(now.Add(e.cfg.lookahead)) {
			wake := minDue.Add(-e.cfg.lookahead)
			if cap := now.Add(e.cfg.misfireHalo / 2); wake.After(cap) {
				wake = cap
			}
			e.setQuietUntil(wake)
		}
		return nil
	}
	e.setQuietUntil(time.Time{}) // window has work: watermark cleared
	ids := make([]string, len(cands))
	for i, c := range cands {
		ids[i] = c.ID
	}
	// acquire in bounded chunks: SQL drivers cap placeholders (SQLite
	// ~32K vars; MySQL packet limits) — admission batches stay portable
	won := make([]Job, 0, len(ids))
	const acquireChunk = 500
	for off := 0; off < len(ids); off += acquireChunk {
		end := off + acquireChunk
		if end > len(ids) {
			end = len(ids)
		}
		part, err := e.store.Acquire(ctx, e.unit, e.epoch, ids[off:end])
		if err != nil {
			return fmt.Errorf("admission acquire: %w", err)
		}
		won = append(won, part...)
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

// pump advances the wheel to now, batch-advances the fired batch's
// state (one statement per tick per target state), then dispatches
// entries to the worker pool (handlers never run under the wheel
// lock). Handler bodies see rows already in fired; completion is
// aggregated per batch: done/dead ids are collected by the workers and
// flushed as one batched transition + one receipt batch each.
func (e *engine) pump(now time.Time) {
	batch := e.wheel.Tick(now)
	if len(batch) == 0 {
		return
	}
	ctx := context.Background()
	ids := make([]string, len(batch))
	for i, en := range batch {
		ids[i] = en.id
	}
	// acquired -> fired for the whole batch (canceled rows simply lose)
	if err := e.transitionBatch(ctx, Acquired, Fired, ids, now); err != nil {
		return
	}
	_ = e.store.attemptFor(ctx, ids) // attempts+1, batched

	// one re-read for the whole batch: handlers get their job row without
	// a per-job Get
	jobs, err := e.store.getMany(ctx, ids)
	if err != nil {
		return
	}
	now2 := now
	// filter raced-away rows (canceled between transition and re-read)
	// BEFORE arming the collector: the expected count must be exact or
	// the flush never fires
	firedIDs := make([]string, 0, len(batch))
	deliver := make([]wheelTask, 0, len(batch))
	for _, entry := range batch {
		j, ok := jobs[entry.id]
		if !ok || j.State != Fired {
			continue // never handed to a handler: no delivery, no death
		}
		firedIDs = append(firedIDs, j.ID)
		deliver = append(deliver, wheelTask{job: j})
	}
	if len(firedIDs) > 0 {
		// one receipt per fired job with per-job attempt numbers (one
		// multi-VALUES insert); rows re-read after the attempt bump
		_ = e.store.addReceiptAttempt(ctx, firedIDs, jobs, "fired", now)
	}
	if len(deliver) == 0 {
		return
	}
	res := newBatchResult(len(deliver))
	for i := range deliver {
		deliver[i].res = res
	}
	// Non-blocking dispatch: a full dispatch channel must never stall
	// the tick loop (review finding #2). Overflowing tasks are released
	// back to waiting — the anti-loss edge — and re-admitted when
	// workers drain. select on done also makes Close race-free: no send
	// can outrun close(dispatch).
	var spill []string
	for i := range deliver {
		select {
		case e.dispatch <- deliver[i]:
		case <-e.done:
			spill = append(spill, deliver[i].job.ID)
		default:
			spill = append(spill, deliver[i].job.ID)
		}
	}
	if len(spill) > 0 {
		// tasks that never reached a handler: count them out of the
		// batch so the flush fires for the delivered remainder
		res.shrink(len(spill))
		_ = e.transitionBatch(ctx, Fired, Waiting, spill, now)
		_ = e.store.attemptForRollback(ctx, spill)
	}
	res.setFlush(func(doneIds, deadIds []string, deadErrs map[string]string) {
		_ = now2
		if len(doneIds) > 0 {
			_ = e.transitionBatch(ctx, Fired, Completed, doneIds, now2)
			_ = e.store.addReceiptAttempt(ctx, doneIds, jobs, "completed", now2)
			for _, id := range doneIds {
				e.maybeReschedule(ctx, id)
			}
		}
		if len(deadIds) > 0 {
			// split: exhausted -> dead; has-attempts-left -> retry with
			// backoff (fired -> waiting)
			var exhausted, retry []string
			for _, id := range deadIds {
				j, ok := jobs[id]
				if ok && j.Attempts < j.MaxAttempts {
					retry = append(retry, id)
				} else {
					exhausted = append(exhausted, id)
				}
			}
			if len(exhausted) > 0 {
				_ = e.transitionBatch(ctx, Fired, Dead, exhausted, now2)
			}
			if len(retry) > 0 {
				for _, id := range retry {
					_ = e.store.retryFor(ctx, id, now2, jobs[id].Backoff)
				}
			}
			// receipts: dead or retry kind, grouped by error text
			byErr := make(map[string][]string)
			for id, txt := range deadErrs {
				byErr[txt] = append(byErr[txt], id)
			}
			for txt, group := range byErr {
				var deadGroup, retryGroup []string
				for _, id := range group {
					if j, ok := jobs[id]; ok && j.Attempts < j.MaxAttempts {
						retryGroup = append(retryGroup, id)
					} else {
						deadGroup = append(deadGroup, id)
					}
				}
				if len(deadGroup) > 0 {
					_ = e.store.addReceiptAttemptErr(ctx, deadGroup, jobs, "dead", txt, now2)
				}
				if len(retryGroup) > 0 {
					_ = e.store.addReceiptAttemptErr(ctx, retryGroup, jobs, "retry", txt, now2)
				}
			}
		}
	})
}

// batchResult collects per-job handler outcomes for one pump batch and
// flushes them once, batched.
type batchResult struct {
	pending atomic.Int64
	mu      sync.Mutex
	doneIds []string
	deadErr map[string]string // id -> error text
	flushed bool
	fn      func(doneIds, deadIds []string, deadErrs map[string]string)
}

func newBatchResult(n int) *batchResult {
	br := &batchResult{deadErr: map[string]string{}}
	br.pending.Store(int64(n))
	return br
}

// shrink removes k expected outcomes (tasks spilled back to waiting
// before dispatch); if nothing remains, flush fires immediately.
func (r *batchResult) shrink(k int) {
	if r.pending.Add(-int64(k)) <= 0 {
		r.flush()
	}
}

func (r *batchResult) complete(id string, herr error) {
	r.mu.Lock()
	if herr == nil {
		r.doneIds = append(r.doneIds, id)
	} else {
		r.deadErr[id] = herr.Error()
	}
	r.mu.Unlock()
	if r.pending.Add(-1) == 0 {
		r.flush()
	}
}

// setFlush installs the flush callback; if all outcomes already
// arrived, flush immediately.
func (r *batchResult) setFlush(fn func(doneIds, deadIds []string, deadErrs map[string]string)) {
	r.mu.Lock()
	r.fn = fn
	r.mu.Unlock()
	if r.pending.Load() == 0 {
		r.flush()
	}
}

func (r *batchResult) flush() {
	r.mu.Lock()
	if r.flushed || r.fn == nil {
		r.mu.Unlock()
		return
	}
	r.flushed = true
	fn, doneIds := r.fn, r.doneIds
	deadCopy := make(map[string]string, len(r.deadErr))
	for k, v := range r.deadErr {
		deadCopy[k] = v
	}
	var deadIds []string
	for id := range deadCopy {
		deadIds = append(deadIds, id)
	}
	r.mu.Unlock()
	fn(doneIds, deadIds, deadCopy)
}

// wheelTask pairs a fired job with its batch result collector.
type wheelTask struct {
	job Job
	res *batchResult
}

// fireEntry is the dispatch path: run the handler on the carried job
// row and report the outcome to the batch collector. State writes
// happen in the batch flush (one statement per tick per target state),
// not per job — the hot minute must not be a write storm.
func (e *engine) fireEntry(task wheelTask) {
	ctx := context.Background()
	j := task.job
	h := e.handlerFor(j.Topic)
	e.mu.Lock()
	ex := e.exec
	e.mu.Unlock()
	if ex != nil {
		// remote execution face: push to the executor, ack by fired-id
		rescheduleAt, err := e.deliver(j, j.ID)
		if rescheduleAt > 0 && err == nil {
			// self-chaining: complete this occurrence, requeue for the
			// acked time once the flush has completed the row
			e.rescheduleAfter(ctx, j, rescheduleAt)
			task.res.complete(j.ID, nil)
			return
		}
		task.res.complete(j.ID, err)
		return
	}
	var herr error
	if h == nil {
		herr = fmt.Errorf("hourglass: no handler for topic %q", j.Topic)
	} else {
		herr = e.runHandler(ctx, h, j)
	}
	task.res.complete(j.ID, herr)
}

// rescheduleAfter requeues a fired job for an acked reschedule_at
// (self-chaining): waits for the batch flush to land completed, then
// flips it to waiting at the acked time.
func (e *engine) rescheduleAfter(ctx context.Context, j Job, at int64) {
	time.AfterFunc(300*time.Millisecond, func() {
		if err := e.store.transition(ctx, j.ID, Completed, Waiting); err == nil {
			_ = e.store.setDue(ctx, j.ID, time.Unix(at, 0).UTC())
		}
	})
}

// runHandler contains panics: a panicking handler is an error on that
// job, never a dead worker or a wedged batch (review finding #4).
func (e *engine) runHandler(ctx context.Context, h Handler, j Job) (herr error) {
	defer func() {
		if r := recover(); r != nil {
			herr = fmt.Errorf("hourglass: handler for %q panicked: %v", j.Topic, r)
		}
	}()
	return h(ctx, j)
}

func (e *engine) handlerFor(topic string) Handler {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.handlers[topic]
}

// transition advances a job's state atomically: UPDATE ... WHERE
// state = from. Illegal edges and lost races fail cleanly.
func (e *engine) transition(ctx context.Context, id string, from, to State) error {
	return e.store.transition(ctx, id, from, to)
}

// transitionBatch is the batched form (one statement per tick).
func (e *engine) transitionBatch(ctx context.Context, from, to State, ids []string, at time.Time) error {
	return e.store.transitionBatch(ctx, from, to, ids, at)
}

// stmtCounter is the test-only store probe for write counts.
type stmtCounter interface{ stmtCount() int64 }

// stmtCount exposes the store's statement counter (tests only).
func (e *engine) stmtCount() int64 {
	if c, ok := e.store.(stmtCounter); ok {
		return c.stmtCount()
	}
	return 0
}

// statsQuerier is the CLI store probe for stats.
type statsQuerier interface {
	statsByState(ctx context.Context) *Stats
}

// storeStats summarizes jobs by state for `hourglass stats`.
func (e *engine) storeStats(ctx context.Context) *Stats {
	var db *sql.DB
	if p, ok := e.store.(interface{ rawDB() *sql.DB }); ok {
		db = p.rawDB()
	}
	if db == nil {
		return &Stats{}
	}
	rows, err := db.QueryContext(ctx,
		`SELECT state, COUNT(*) FROM jobs GROUP BY state ORDER BY state`)
	if err != nil {
		return &Stats{}
	}
	defer rows.Close()
	st := &Stats{}
	for rows.Next() {
		var sc StateCount
		var state string
		if err := rows.Scan(&state, &sc.Count); err != nil {
			return st
		}
		sc.State = State(state)
		st.JobsByState = append(st.JobsByState, sc)
		st.TotalJobs += sc.Count
	}
	_ = db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM receipts`).Scan(&st.TotalReceipts)
	var oldest sql.NullInt64
	_ = db.QueryRowContext(ctx,
		`SELECT MIN(terminal_at) FROM jobs WHERE state IN ('completed','dead','canceled') AND terminal_at > 0`).Scan(&oldest)
	if oldest.Valid && oldest.Int64 > 0 {
		st.OldestTerminal = time.Unix(oldest.Int64, 0).UTC()
	}
	return st
}

// deadLister is the CLI store probe.
type deadLister interface {
	listDead(ctx context.Context, topic string, limit int) ([]Job, error)
	listDeadIDs(ctx context.Context) ([]string, error)
}

// listDead lists dead rows newest-first for `hourglass dlq ls`.
func (e *engine) listDead(ctx context.Context, topic string, limit int) ([]Job, error) {
	if d, ok := e.store.(deadLister); ok {
		return d.listDead(ctx, topic, limit)
	}
	return nil, fmt.Errorf("hourglass: store does not support dlq listing")
}

// replayDead requeues one dead row: attempts reset, due = now.
func (e *engine) replayDead(ctx context.Context, id string) error {
	r, ok := e.store.(interface {
		replayDead(ctx context.Context, id string, now time.Time) error
	})
	if !ok {
		return fmt.Errorf("hourglass: store does not support dlq replay")
	}
	now := e.clk.Now()
	if err := r.replayDead(ctx, id, now); err != nil {
		return err
	}
	e.setQuietUntil(time.Time{})
	return nil
}

// replayAllDead requeues every dead row; returns the count.
func (e *engine) replayAllDead(ctx context.Context) (int, error) {
	d, ok := e.store.(deadLister)
	if !ok {
		return 0, fmt.Errorf("hourglass: store does not support dlq listing")
	}
	ids, err := d.listDeadIDs(ctx)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, id := range ids {
		if err := e.replayDead(ctx, id); err == nil {
			n++
		}
	}
	if n > 0 {
		e.setQuietUntil(time.Time{})
	}
	return n, nil
}

// TestStep and TestPump expose the admission/fire loop to package tests
// (CLI integration tests drive a real Handle deterministically).
func (e *engine) TestStep(ctx context.Context) error { return e.step(ctx) }

func (e *engine) TestPump(now time.Time) { e.pump(now) }

// readCount exposes the store's read counter (benchmarks only).
func (e *engine) readCount() int64 {
	if c, ok := e.store.(interface{ readCount() int64 }); ok {
		return c.readCount()
	}
	return 0
}

// sweep runs one retention pass: terminal rows past the TTL leave in
// bounded batches. Never inline with the fire path.
func (e *engine) sweep(ctx context.Context, now time.Time) (int, error) {
	if p, ok := e.store.(interface {
		purgeTerminal(ctx context.Context, now time.Time, ttl time.Duration, limit int) (int, error)
	}); ok {
		return p.purgeTerminal(ctx, now, e.cfg.retention, maxSweepPerPass)
	}
	return 0, nil
}

// Run drives admission and wheel ticks on a one-second cadence until
// ctx is done.
func (e *engine) Run(ctx context.Context) {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	sweepEvery := 10 * time.Minute
	nextSweep := time.Now().Add(sweepEvery)
	for {
		select {
		case <-ctx.Done():
			return
		case <-e.done:
			return
		case now := <-tick.C:
			e.pump(now)
			if err := e.step(ctx); err != nil {
				// admission errors are survivable: next tick retries
				continue
			}
			if now.After(nextSweep) {
				nextSweep = now.Add(sweepEvery)
				_, _ = e.sweep(ctx, now)
			}
		}
	}
}

// Close stops the engine: signal done (Run and pump stop), close the
// dispatch channel once no sender can race it, wait for handlers, then
// release the wheel. Idempotent.
func (e *engine) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	e.mu.Unlock()

	close(e.done)     // pump selects on this; Run's loop sees it via ctx or done
	close(e.dispatch) // safe now: pump's only send path selects on done
	e.wg.Wait()       // workers drain dispatch then exit
	e.closeExecutor()
	return e.wheel.Close()
}
