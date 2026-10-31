package hourglass

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func newTestStore(t *testing.T) *sqliteStore {
	t.Helper()
	s, err := newSQLiteStore(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("newSQLiteStore: %v", err)
	}
	t.Cleanup(func() { s.db.Close() })
	return s
}

func mustSubmit(t *testing.T, s Store, topic string, due time.Time) string {
	t.Helper()
	j := &Job{Topic: topic, Due: due, State: Waiting}
	if err := s.Insert(context.Background(), j); err != nil {
		t.Fatalf("insert %s: %v", topic, err)
	}
	return j.ID
}

func TestSelectReturnsWaitingJobsInDueOrder(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	base := time.Date(2026, 11, 1, 9, 0, 0, 0, time.UTC)

	mustSubmit(t, s, "late", base.Add(2*time.Minute))
	mustSubmit(t, s, "early", base.Add(1*time.Minute))
	idDone := mustSubmit(t, s, "done", base.Add(30*time.Second))
	// a non-waiting job must never be selected
	if _, err := s.db.Exec(`UPDATE jobs SET state='completed' WHERE id=?`, idDone); err != nil {
		t.Fatal(err)
	}

	got, err := s.Select(ctx, scanUnitAll, base, base.Add(5*time.Minute), 10)
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("Select returned %d jobs, want 2", len(got))
	}
	if got[0].Topic != "early" || got[1].Topic != "late" {
		t.Errorf("order = %s,%s; want early,late", got[0].Topic, got[1].Topic)
	}
}

func TestSelectRespectsLimit(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	base := time.Date(2026, 11, 1, 9, 0, 0, 0, time.UTC)
	mustSubmit(t, s, "a", base)
	mustSubmit(t, s, "b", base.Add(time.Second))
	mustSubmit(t, s, "c", base.Add(2*time.Second))

	got, err := s.Select(ctx, scanUnitAll, base, base.Add(time.Minute), 2)
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	if len(got) != 2 || got[0].Topic != "a" || got[1].Topic != "b" {
		t.Fatalf("limit-2 Select = %v", got)
	}
}

func TestAcquireIsCAS(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	base := time.Date(2026, 11, 1, 9, 0, 0, 0, time.UTC)
	idA := mustSubmit(t, s, "a", base)
	idB := mustSubmit(t, s, "b", base)

	won, err := s.Acquire(ctx, scanUnitAll, 1, []string{idA, idB})
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if len(won) != 2 {
		t.Fatalf("Acquire won %d, want 2", len(won))
	}
	for _, j := range won {
		if j.State != Acquired {
			t.Errorf("job %s state after acquire = %s, want acquired", j.ID, j.State)
		}
	}

	// second acquire of the same ids must win nothing: state is no longer waiting
	won2, err := s.Acquire(ctx, scanUnitAll, 2, []string{idA, idB})
	if err != nil {
		t.Fatalf("second Acquire: %v", err)
	}
	if len(won2) != 0 {
		t.Errorf("second Acquire won %d, want 0 (CAS)", len(won2))
	}
}
