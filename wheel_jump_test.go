package hourglass

import (
	"testing"
	"time"
)

func TestWheelJumpTickDoesNotLoseEntry(t *testing.T) {
	rec, fired := newTestWheel(1)
	w := rec.w
	base := w.clk.Now()
	w.Offer(wheelEntry{id: "x", due: base.Add(65 * time.Second), epoch: 1})
	rec.tick(base.Add(60 * time.Second)) // direct jump, no intermediate ticks
	if len(*fired) != 0 {
		t.Fatalf("fired early at jump: %v", *fired)
	}
	rec.tick(base.Add(65 * time.Second))
	if len(*fired) != 1 {
		t.Fatalf("jump tick lost the entry: %v", *fired)
	}
}
