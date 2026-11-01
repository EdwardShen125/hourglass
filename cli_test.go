package hourglass_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/EdwardShen125/hourglass"
)

// CLI surface (RED first): Stats, ListDead, ReplayDead, ReplayAllDead.

type fakeCLIClock struct{ now time.Time }

func (c *fakeCLIClock) Now() time.Time { return c.now }

func newCLITestHandle(t *testing.T) (*hourglass.Handle, *fakeCLIClock) {
	t.Helper()
	clk := &fakeCLIClock{now: time.Date(2026, 11, 1, 9, 0, 0, 0, time.UTC)}
	h, err := hourglass.OpenWithClock("sqlite://"+filepath.Join(t.TempDir(), "cli.db"), clk)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.Close() })
	return h, clk
}

func TestStatsCountsByState(t *testing.T) {
	h, _ := newCLITestHandle(t)
	ctx := context.Background()
	h.Register("ok", func(context.Context, hourglass.Job) error { return nil })
	h.Register("boom", func(context.Context, hourglass.Job) error { return errCLIBoom })

	base := time.Date(2026, 11, 1, 9, 0, 0, 0, time.UTC)
	// 2 waiting, 1 completed, 1 dead, 1 canceled
	h.Submit(ctx, "ok", nil, hourglass.At(base.Add(2*time.Hour)))
	h.Submit(ctx, "ok", nil, hourglass.At(base.Add(3*time.Hour)))
	idDone, _ := h.Submit(ctx, "ok", nil, hourglass.At(base.Add(-time.Minute)))
	idDead, _ := h.Submit(ctx, "boom", nil, hourglass.At(base.Add(-2*time.Minute)))
	idCancel, _ := h.Submit(ctx, "ok", nil, hourglass.At(base.Add(time.Hour)))

	// drive the engine manually: admit+fire past-due ones
	h.Eng().TestStep(ctx)                 // admit all inside window
	h.Eng().TestPump(base.Add(time.Hour)) // fire due ones
	waitCLIState(t, h, idDone, hourglass.Completed)
	waitCLIState(t, h, idDead, hourglass.Dead)
	if err := h.Cancel(ctx, idCancel, "cli test"); err != nil {
		t.Fatal(err)
	}

	st, err := h.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]int{
		"waiting": 2, "completed": 1, "dead": 1, "canceled": 1,
	}
	got := map[string]int{}
	for _, line := range st.JobsByState {
		got[string(line.State)] = int(line.Count)
	}
	for state, n := range want {
		if got[state] != n {
			t.Errorf("state %s = %d, want %d (got %v)", state, got[state], n, got)
		}
	}
	if st.TotalJobs != 5 {
		t.Errorf("total = %d, want 5", st.TotalJobs)
	}
	if st.TotalReceipts < 4 { // fired+completed, fired+dead, canceled
		t.Errorf("receipts = %d, want >= 4", st.TotalReceipts)
	}
}

func TestListDeadFiltersAndLimits(t *testing.T) {
	h, _ := newCLITestHandle(t)
	ctx := context.Background()
	h.Register("boom", func(context.Context, hourglass.Job) error { return errCLIBoom })
	h.Register("boom2", func(context.Context, hourglass.Job) error { return errCLIBoom })

	base := time.Date(2026, 11, 1, 9, 0, 0, 0, time.UTC)
	var ids []string
	for i := 0; i < 5; i++ {
		topic := "boom"
		if i >= 3 {
			topic = "boom2"
		}
		id, _ := h.Submit(ctx, topic, nil, hourglass.At(base.Add(time.Duration(i)*time.Second-time.Minute)))
		ids = append(ids, id)
	}
	h.Eng().TestStep(ctx)
	h.Eng().TestPump(base)
	for _, id := range ids {
		waitCLIState(t, h, id, hourglass.Dead)
	}

	// no filter: all 5, newest-first? assert set + limit behavior
	all, err := h.ListDead(ctx, "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 5 {
		t.Fatalf("ListDead all = %d, want 5", len(all))
	}
	// topic filter
	only2, err := h.ListDead(ctx, "boom2", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(only2) != 2 {
		t.Fatalf("ListDead boom2 = %d, want 2", len(only2))
	}
	for _, j := range only2 {
		if j.Topic != "boom2" {
			t.Errorf("filter leaked topic %q", j.Topic)
		}
	}
	// limit
	lim, err := h.ListDead(ctx, "", 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(lim) != 2 {
		t.Fatalf("ListDead limit = %d, want 2", len(lim))
	}
}

func TestReplayDeadRequeuesAndRefires(t *testing.T) {
	h, _ := newCLITestHandle(t)
	ctx := context.Background()

	var boomRuns, okRuns int
	h.Register("flaky", func(context.Context, hourglass.Job) error {
		boomRuns++
		if boomRuns <= 1 {
			return errCLIBoom // fails the first run only
		}
		okRuns++
		return nil
	})

	base := time.Date(2026, 11, 1, 9, 0, 0, 0, time.UTC)
	id, _ := h.Submit(ctx, "flaky", nil, hourglass.At(base.Add(-time.Minute)))
	h.Eng().TestStep(ctx)
	h.Eng().TestPump(base)
	waitCLIState(t, h, id, hourglass.Dead)

	// replay: dead -> waiting, attempts reset, due = now-ish
	if err := h.ReplayDead(ctx, id); err != nil {
		t.Fatalf("ReplayDead: %v", err)
	}
	j, _ := h.Get(ctx, id)
	if j.State != hourglass.Waiting {
		t.Fatalf("after replay: state = %s, want waiting", j.State)
	}
	if j.Attempts != 0 {
		t.Errorf("after replay: attempts = %d, want reset to 0", j.Attempts)
	}

	// the engine fires it again and the (now-healthy) handler completes
	h.Eng().TestStep(ctx)
	h.Eng().TestPump(base.Add(time.Hour))
	waitCLIState(t, h, id, hourglass.Completed)
	if okRuns != 1 {
		t.Errorf("handler ok-runs = %d, want 1", okRuns)
	}
}

func TestReplayDeadRejectsNonDead(t *testing.T) {
	h, _ := newCLITestHandle(t)
	ctx := context.Background()
	h.Register("ok", func(context.Context, hourglass.Job) error { return nil })
	base := time.Date(2026, 11, 1, 9, 0, 0, 0, time.UTC)
	id, _ := h.Submit(ctx, "ok", nil, hourglass.At(base.Add(time.Hour)))
	if err := h.ReplayDead(ctx, id); err == nil {
		t.Fatal("ReplayDead on a waiting job returned nil, want error")
	}
}

func TestReplayAllDead(t *testing.T) {
	h, _ := newCLITestHandle(t)
	ctx := context.Background()
	h.Register("boom", func(context.Context, hourglass.Job) error { return errCLIBoom })
	h.Register("ok", func(context.Context, hourglass.Job) error { return nil })
	base := time.Date(2026, 11, 1, 9, 0, 0, 0, time.UTC)

	var deadIDs []string
	for i := 0; i < 3; i++ {
		id, _ := h.Submit(ctx, "boom", nil, hourglass.At(base.Add(time.Duration(i)*time.Second-time.Minute)))
		deadIDs = append(deadIDs, id)
	}
	okID, _ := h.Submit(ctx, "ok", nil, hourglass.At(base.Add(2*time.Hour)))
	h.Eng().TestStep(ctx)
	h.Eng().TestPump(base)
	for _, id := range deadIDs {
		waitCLIState(t, h, id, hourglass.Dead)
	}

	n, err := h.ReplayAllDead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("ReplayAllDead = %d, want 3", n)
	}
	for _, id := range deadIDs {
		j, _ := h.Get(ctx, id)
		if j.State != hourglass.Waiting {
			t.Errorf("%s not requeued: %s", id, j.State)
		}
	}
	// non-dead untouched
	j, _ := h.Get(ctx, okID)
	if j.State != hourglass.Waiting {
		t.Errorf("waiting job disturbed by --all: %s", j.State)
	}
}

var errCLIBoom = &cliError{"boom"}

type cliError struct{ s string }

func (e *cliError) Error() string { return e.s }

func waitCLIState(t *testing.T, h *hourglass.Handle, id string, want hourglass.State) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		j, err := h.Get(context.Background(), id)
		if err == nil && j.State == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	j, _ := h.Get(context.Background(), id)
	state := "nil"
	if j != nil {
		state = string(j.State)
	}
	t.Fatalf("job %s never reached %s (stuck in %s)", id, want, state)
}
