package hourglass

import (
	"context"
	"testing"
	"time"
)

func TestCronParsesAndComputesNext(t *testing.T) {
	base := time.Date(2026, 11, 1, 9, 0, 2, 0, time.UTC) // 09:00:02 Sunday

	cs, err := parseCron("*/5 * * * *")
	if err != nil {
		t.Fatal(err)
	}
	if got := cs.nextAfter(base); !got.Equal(time.Date(2026, 11, 1, 9, 5, 0, 0, time.UTC)) {
		t.Errorf("*/5 next = %v, want 09:05:00", got)
	}

	cd, err := parseCron("0 9 * * *")
	if err != nil {
		t.Fatal(err)
	}
	if got := cd.nextAfter(base); !got.Equal(time.Date(2026, 11, 2, 9, 0, 0, 0, time.UTC)) {
		t.Errorf("daily-9am next = %v, want next day 09:00", got)
	}

	// dom/dow OR rule (standard cron): the 1st OR any Sunday
	cm, err := parseCron("0 0 1 * 0")
	if err != nil {
		t.Fatal(err)
	}
	// Nov 2 is Monday; Nov 1 is Sunday and also the 1st — from Nov 2, the
	// next match is Nov 8 (Sunday), before Dec 1.
	if got := cm.nextAfter(time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC)); !got.Equal(time.Date(2026, 11, 8, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("dom|dow next = %v, want Nov 8", got)
	}
}

func TestCronRejectsBadSpec(t *testing.T) {
	for _, bad := range []string{"", "* * * *", "61 * * * *", "* 24 * * *", "*/0 * * * *", "a b c d e"} {
		if _, err := parseCron(bad); err == nil {
			t.Errorf("parseCron(%q) accepted, want error", bad)
		}
	}
}

func TestEveryReschedulesAfterCompletion(t *testing.T) {
	s := newTestStore(t)
	clk := &fakeClock{now: time.Date(2026, 11, 1, 9, 0, 0, 0, time.UTC)}
	e := newEngine(s, clk, engineConfig{lookahead: 10 * time.Minute, admitLimit: 100})
	ctx := context.Background()

	var fired int
	e.Register("report:5m", func(context.Context, Job) error { fired++; return nil })

	id, err := e.Every(ctx, "report:5m", "*/5 * * * *")
	if err != nil {
		t.Fatal(err)
	}
	j, _ := e.Get(ctx, id)
	if want := time.Date(2026, 11, 1, 9, 5, 0, 0, time.UTC); !j.Due.Equal(want) {
		t.Fatalf("first due = %v, want %v", j.Due, want)
	}

	if err := e.step(ctx); err != nil {
		t.Fatal(err)
	}
	e.pump(clk.Now().Add(5 * time.Minute))
	waitJobState(t, e, id, Waiting)

	j, _ = e.Get(ctx, id)
	if want := time.Date(2026, 11, 1, 9, 10, 0, 0, time.UTC); !j.Due.Equal(want) {
		t.Fatalf("rescheduled due = %v, want %v", j.Due, want)
	}
	if fired != 1 {
		t.Fatalf("fired %d times, want 1", fired)
	}
}

func TestCronDeadDoesNotReschedule(t *testing.T) {
	// DESIGN: fired -> dead is terminal; only completed -> waiting (cron
	// next occurrence) exists.
	s := newTestStore(t)
	clk := &fakeClock{now: time.Date(2026, 11, 1, 9, 0, 0, 0, time.UTC)}
	e := newEngine(s, clk, engineConfig{lookahead: 10 * time.Minute, admitLimit: 100})
	ctx := context.Background()

	e.Register("boom", func(context.Context, Job) error { return errTestBoom })
	id, err := e.Every(ctx, "boom", "*/5 * * * *")
	if err != nil {
		t.Fatal(err)
	}

	e.step(ctx)
	e.pump(clk.Now().Add(5 * time.Minute))
	waitJobState(t, e, id, Dead)

	j, _ := e.Get(ctx, id)
	if j.State != Dead {
		t.Fatalf("cron job after handler failure: state = %s, want dead (terminal)", j.State)
	}
}

var errTestBoom = &testError{"boom"}

type testError struct{ s string }

func (e *testError) Error() string { return e.s }
