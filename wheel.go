package hourglass

import (
	"errors"
	"sync"
	"time"
)

// ErrWheelFull is returned by Offer when the wheel is at capacity; the
// admission loop must release the surplus back to waiting.
var ErrWheelFull = errors.New("hourglass: wheel at capacity")

// clock is the time seam; production uses realClock, tests use fakes.
type clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// wheelEntry is one admitted-but-unfired job inside the wheel.
type wheelEntry struct {
	id       string
	due      time.Time
	epoch    uint64
	canceled bool
}

// wheel is a hierarchical timing wheel: 60 slots per level, level L
// covering 60^L-second spans. Insert O(1); advancement processes only
// the buckets actually passed — O(1) amortized per tick. Entries due
// beyond their level's horizon cascade down as time advances.
//
// Correctness machinery (DESIGN.md "four load-bearing pieces"):
//   - Epoch validation happens on the fire path, per batch: an entry
//     admitted under a revoked epoch is dropped, not delivered.
//   - Cancel is mark-don't-remove: racing the tick with a removal risks
//     a lost update; a late mark is a harmless no-op.
type wheel struct {
	mu       sync.Mutex
	clk      clock
	epoch    uint64
	capacity int
	size     int
	now      time.Time
	closed   bool
	index    map[string]*wheelEntry
	levels   []*wheelLevel
	// ready holds entries whose due is at or before the wheel's current
	// time (misfires admitted by the rescan horizon). Bucket indexes in
	// the past are never revisited, so past-due entries ride ready and
	// fire on the next tick, however small the advance.
	ready []*wheelEntry
}

// wheelLevel holds buckets keyed by absolute bucket index
// (dueUnix / unit). unit = 60^L seconds; the level spans 60 buckets.
type wheelLevel struct {
	unit    int64
	buckets map[int64][]*wheelEntry
}

var wheelSize int64 = 60

func newWheel(clk clock, epoch uint64) *wheel {
	w := &wheel{
		clk:      clk,
		epoch:    epoch,
		capacity: 1 << 20,
		index:    make(map[string]*wheelEntry),
		now:      clk.Now().Truncate(time.Second),
	}
	unit := int64(1)
	for i := 0; i < 8; i++ {
		w.levels = append(w.levels, &wheelLevel{unit: unit, buckets: make(map[int64][]*wheelEntry)})
		unit *= wheelSize
	}
	return w
}

// Offer admits one entry. Returns ErrWheelFull when at capacity; the
// entry is then not held and must be released by the caller.
func (w *wheel) Offer(e wheelEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return errors.New("hourglass: wheel closed")
	}
	if w.size >= w.capacity {
		return ErrWheelFull
	}
	ent := e
	w.insert(&ent)
	w.index[e.id] = &ent
	w.size++
	return nil
}

// insert places an entry at the lowest level whose horizon covers it.
// Past-due entries go to the ready queue instead — bucket indexes in
// the past are never revisited. Caller holds w.mu.
func (w *wheel) insert(e *wheelEntry) {
	remaining := e.due.Unix() - w.now.Unix()
	if remaining <= 0 {
		w.ready = append(w.ready, e)
		return
	}
	for _, lvl := range w.levels {
		interval := lvl.unit * wheelSize
		if remaining < interval {
			idx := e.due.Unix() / lvl.unit
			lvl.buckets[idx] = append(lvl.buckets[idx], e)
			return
		}
	}
	// Beyond the top level's horizon: park in the last level's farthest
	// bucket; cascades will bring it down. Horizon is years; unreachable
	// in practice, but lossless beats panicking.
	top := w.levels[len(w.levels)-1]
	idx := e.due.Unix() / top.unit
	top.buckets[idx] = append(top.buckets[idx], e)
}

// Cancel marks an admitted-but-unfired entry canceled. Returns false
// when the id is unknown or already fired — cancel after fire is not a
// thing.
func (w *wheel) Cancel(id string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if e, ok := w.index[id]; ok {
		e.canceled = true
		return true
	}
	return false
}

// Tick advances the wheel to now, returning the entries to fire.
// Handlers run OUTSIDE the wheel lock (caller dispatches the returned
// slice), so a slow handler never stalls admission or cancel. Entries
// whose epoch no longer matches the wheel's epoch are dropped (zombie
// fencing); entries in passed buckets but still due later cascade down.
func (w *wheel) Tick(now time.Time) []wheelEntry {
	w.mu.Lock()
	defer w.mu.Unlock()
	now = now.Truncate(time.Second)
	if now.Before(w.now) {
		return nil
	}
	var due []wheelEntry
	// ready fires first: misfires and same-second admissions must not
	// wait for the clock to move.
	if len(w.ready) > 0 {
		batch := w.ready
		w.ready = nil
		due = append(due, w.collectBatch(batch, now)...)
	}
	if !now.After(w.now) {
		return due
	}
	for _, lvl := range w.levels {
		prevIdx := w.now.Unix() / lvl.unit
		curIdx := now.Unix() / lvl.unit
		for idx := prevIdx + 1; idx <= curIdx; idx++ {
			batch := lvl.buckets[idx]
			if len(batch) == 0 {
				continue
			}
			delete(lvl.buckets, idx)
			due = append(due, w.collectBatch(batch, now)...)
		}
	}
	w.now = now
	return due
}

// collectBatch filters one bucket batch: epoch check first (per batch,
// before firing), returning entries due now; still-future entries
// cascade down. Caller holds w.mu and dispatches the returns outside
// the lock.
func (w *wheel) collectBatch(batch []*wheelEntry, now time.Time) []wheelEntry {
	var due []wheelEntry
	for _, e := range batch {
		switch {
		case e.canceled:
			w.retire(e)
		case e.epoch != w.epoch:
			// Zombie: ownership was revoked; must not deliver.
			w.retire(e)
		case e.due.Unix() <= now.Unix():
			w.retire(e)
			due = append(due, *e)
		default:
			w.insert(e) // cascade: due is nearer now, lands at a lower level
		}
	}
	return due
}

// retire removes an entry from the live index. Caller holds w.mu.
func (w *wheel) retire(e *wheelEntry) {
	if _, ok := w.index[e.id]; ok {
		delete(w.index, e.id)
		w.size--
	}
}

// Drain returns ids of live (admitted-but-unfired) entries — for
// graceful handoff: fire what is due, requeue the rest.
func (w *wheel) Drain() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	ids := make([]string, 0, len(w.index))
	for id := range w.index {
		ids = append(ids, id)
	}
	return ids
}

// Close stops accepting new entries. In-flight entries stay queryable
// via Drain until consumed.
func (w *wheel) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closed = true
	return nil
}
