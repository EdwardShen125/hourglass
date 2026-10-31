package hourglass

import (
	"fmt"
	"testing"
	"time"
)

type fakeClock struct{ now time.Time }

func (c *fakeClock) Now() time.Time { return c.now }
func (c *fakeClock) Advance(d time.Duration) {
	c.now = c.now.Add(d)
}

type firedRecord struct {
	id    string
	due   time.Time
	at    time.Time
	epoch uint64
}

func newTestWheel(epoch uint64) (*wheel, *[]firedRecord) {
	fired := &[]firedRecord{}
	clk := &fakeClock{now: time.Date(2026, 11, 1, 9, 0, 0, 0, time.UTC)}
	w := newWheel(clk, epoch, func(e wheelEntry, at time.Time) {
		*fired = append(*fired, firedRecord{id: e.id, due: e.due, at: at, epoch: e.epoch})
	})
	return w, fired
}

func TestWheelFiresAtAppointedSecond(t *testing.T) {
	w, fired := newTestWheel(1)
	base := w.clk.Now()

	w.Offer(wheelEntry{id: "a", due: base.Add(10 * time.Second), epoch: 1})
	w.Offer(wheelEntry{id: "b", due: base.Add(5 * time.Second), epoch: 1})

	w.Tick(base.Add(5 * time.Second))
	if len(*fired) != 1 || (*fired)[0].id != "b" {
		t.Fatalf("after 5s: fired = %v, want [b]", *fired)
	}

	w.Tick(base.Add(10 * time.Second))
	if len(*fired) != 2 || (*fired)[1].id != "a" {
		t.Fatalf("after 10s: fired = %v, want [b a]", *fired)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestWheelNeverFiresEarly(t *testing.T) {
	w, fired := newTestWheel(1)
	base := w.clk.Now()

	w.Offer(wheelEntry{id: "a", due: base.Add(10 * time.Second), epoch: 1})
	w.Tick(base.Add(9 * time.Second))
	if len(*fired) != 0 {
		t.Fatalf("fired early: %v", *fired)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestWheelCancelIsMarkNotRemove(t *testing.T) {
	w, fired := newTestWheel(1)
	base := w.clk.Now()

	w.Offer(wheelEntry{id: "a", due: base.Add(5 * time.Second), epoch: 1})
	w.Offer(wheelEntry{id: "b", due: base.Add(5 * time.Second), epoch: 1})

	if !w.Cancel("a") {
		t.Fatal("Cancel(a) = false, want true (entry admitted)")
	}

	w.Tick(base.Add(5 * time.Second))
	if len(*fired) != 1 || (*fired)[0].id != "b" {
		t.Fatalf("fired = %v, want only [b]; canceled a must be skipped", *fired)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestWheelEpochFencingBlocksZombie(t *testing.T) {
	// zombie node: entries admitted under epoch 1, but ownership moved to 2.
	w, fired := newTestWheel(2)
	base := w.clk.Now()

	w.Offer(wheelEntry{id: "a", due: base.Add(5 * time.Second), epoch: 1})
	w.Tick(base.Add(5 * time.Second))
	if len(*fired) != 0 {
		t.Fatalf("zombie fired: %v — epoch 1 entry must not fire under epoch 2", *fired)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestWheelCascade(t *testing.T) {
	// an entry beyond one wheel revolution must cascade, not be lost
	w, fired := newTestWheel(1)
	base := w.clk.Now()

	w.Offer(wheelEntry{id: "far", due: base.Add(3*time.Hour + 7*time.Minute), epoch: 1})
	// tick well past the due
	end := base.Add(4 * time.Hour)
	for now := base; now.Before(end); now = now.Add(time.Minute) {
		w.Tick(now)
	}
	if len(*fired) != 1 || (*fired)[0].id != "far" {
		t.Fatalf("cascade lost the entry: fired = %v", *fired)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestWheelRejectsWhenFull(t *testing.T) {
	// wheel with capacity 2 must reject the third offer without losing it
	fired := &[]firedRecord{}
	clk := &fakeClock{now: time.Date(2026, 11, 1, 9, 0, 0, 0, time.UTC)}
	w := newWheel(clk, 1, func(e wheelEntry, at time.Time) {
		*fired = append(*fired, firedRecord{id: e.id})
	})
	w.capacity = 2
	base := clk.Now()

	for i := 0; i < 3; i++ {
		err := w.Offer(wheelEntry{id: fmt.Sprintf("j%d", i), due: base.Add(time.Duration(i+1) * time.Second), epoch: 1})
		if i < 2 && err != nil {
			t.Fatalf("Offer %d rejected unexpectedly: %v", i, err)
		}
		if i == 2 && err == nil {
			t.Fatal("third Offer accepted, want ErrWheelFull")
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}
