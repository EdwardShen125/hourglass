package hourglass

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

// Crash recovery: a process that died with rows in acquired/fired must
// have them requeued to waiting at next Open — at-least-once re-fire
// (README Contracts: "a crashed node's in-flight jobs are reclaimed ... and
// re-fired within the misfire horizon").
func TestOpenRecoversInFlightRows(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "recovery.db")

	// first open: create jobs, then simulate a crash by advancing states
	// past waiting WITHOUT running the engine (raw transitions)
	h1, err := Open("sqlite://" + dbPath)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 11, 1, 9, 0, 0, 0, time.UTC)
	idAcquired, err := h1.Submit(ctx, "job", nil, At(base.Add(time.Hour)))
	if err != nil {
		t.Fatal(err)
	}
	idFired, err := h1.Submit(ctx, "job", nil, At(base.Add(time.Hour)))
	if err != nil {
		t.Fatal(err)
	}
	idWaiting, err := h1.Submit(ctx, "job", nil, At(base.Add(2*time.Hour)))
	if err != nil {
		t.Fatal(err)
	}
	s := h1.store.(*sqliteStore)
	// engine-shaped: waiting -> acquired (admission), acquired -> fired (pump)
	if _, err := s.Acquire(ctx, scanUnitAll, 1, []string{idAcquired, idFired}); err != nil {
		t.Fatal(err)
	}
	if err := s.transition(ctx, idFired, Acquired, Fired); err != nil {
		t.Fatal(err)
	}
	// "crash": close without completing
	h1.Close()

	// second open on the same file: recovery must flip in-flight rows
	h2, err := Open("sqlite://" + dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer h2.Close()

	for _, tc := range []struct {
		id   string
		want State
	}{
		{idAcquired, Waiting},
		{idFired, Waiting},
		{idWaiting, Waiting},
	} {
		j, err := h2.Get(ctx, tc.id)
		if err != nil {
			t.Fatal(err)
		}
		if j.State != tc.want {
			t.Errorf("job %s state after recovery = %s, want %s", tc.id, j.State, tc.want)
		}
	}

	// recovered jobs are admissible again: select finds them
	got, err := h2.store.Select(ctx, scanUnitAll, base.Add(-time.Minute), base.Add(3*time.Hour), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("select after recovery = %d, want 3 re-admissible jobs", len(got))
	}
}

// Terminal and canceled rows must NOT be resurrected by recovery.
func TestOpenRecoveryKeepsTerminalRows(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "recovery2.db")

	h1, err := Open("sqlite://" + dbPath)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 11, 1, 9, 0, 0, 0, time.UTC)
	idDone, _ := h1.Submit(ctx, "job", nil, At(base.Add(time.Hour)))
	idDead, _ := h1.Submit(ctx, "job", nil, At(base.Add(time.Hour)))
	idCanceled, _ := h1.Submit(ctx, "job", nil, At(base.Add(time.Hour)))
	s := h1.store.(*sqliteStore)
	acq, err := s.Acquire(ctx, scanUnitAll, 1, []string{idDone, idDead})
	if err != nil {
		t.Fatal(err)
	}
	if len(acq) != 2 {
		t.Fatalf("acquire won %d", len(acq))
	}
	for _, id := range []string{idDone, idDead} {
		if err := s.transition(ctx, id, Acquired, Fired); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.transition(ctx, idDone, Fired, Completed); err != nil {
		t.Fatal(err)
	}
	if err := s.transition(ctx, idDead, Fired, Dead); err != nil {
		t.Fatal(err)
	}
	s1 := h1.store.(*sqliteStore)
	if err := s1.cancel(ctx, idCanceled, "before crash"); err != nil {
		t.Fatal(err)
	}
	h1.Close()

	h2, err := Open("sqlite://" + dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer h2.Close()

	for _, tc := range []struct {
		id   string
		want State
	}{
		{idDone, Completed},
		{idDead, Dead},
		{idCanceled, Canceled},
	} {
		j, err := h2.Get(ctx, tc.id)
		if err != nil {
			t.Fatal(err)
		}
		if j.State != tc.want {
			t.Errorf("terminal job %s resurrected: state = %s, want %s", tc.id, j.State, tc.want)
		}
	}
}
