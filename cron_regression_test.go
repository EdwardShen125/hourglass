package hourglass

import (
	"context"
	"testing"
	"time"
)

// Review round-1 regression tests: cron parser defects.

func TestCronMixedListWithStep(t *testing.T) {
	// "5,7/2" = hour 5 AND hours 7,9,11,...,23 — the bare 5 must NOT
	// expand to 5-23.
	cs, err := parseCron("0 5,7/2 * * *")
	if err != nil {
		t.Fatal(err)
	}
	for h := 0; h <= 23; h++ {
		want := h == 5 || (h >= 7 && h%2 == 1)
		got := cs.hour&(1<<uint(h)) != 0
		if got != want {
			t.Errorf("hour %d: allowed=%v, want %v", h, got, want)
		}
	}
}

func TestCronStepFieldIsStarForV7(t *testing.T) {
	// "0 0 */1 * 1": dom is unrestricted (*/n) — only dow (Monday)
	// restricts. Both-restricted would OR dom into every day.
	cs, err := parseCron("0 0 */1 * 1")
	if err != nil {
		t.Fatal(err)
	}
	if !cs.domStar {
		t.Fatal("*/n in dom must count as unrestricted for the V7 OR rule")
	}
	// from a Wednesday (Nov 4 2026), next must be Monday Nov 9, not
	// tomorrow.
	from := time.Date(2026, 11, 4, 0, 0, 0, 0, time.UTC)
	got := cs.nextAfter(from)
	if want := time.Date(2026, 11, 9, 0, 0, 0, 0, time.UTC); !got.Equal(want) {
		t.Fatalf("*/1 * 1 next = %v, want Monday %v", got, want)
	}
}

func TestCronFeb29ResolvedAndNeverMatchingRejected(t *testing.T) {
	e, clk := newTestEngine(t)
	ctx := context.Background()
	// Feb 29 exists within the 5-year horizon (2028): accepted, first
	// due 2028-02-29 — NOT a silent zero-time row.
	id, err := e.Every(ctx, "leap", "0 0 29 2 *")
	if err != nil {
		t.Fatalf("Feb-29 spec rejected: %v", err)
	}
	j, _ := e.Get(ctx, id)
	if want := time.Date(2028, 2, 29, 0, 0, 0, 0, clk.Now().Location()); !j.Due.Equal(want) {
		t.Fatalf("Feb-29 due = %v, want %v", j.Due, want)
	}
	// never-matching spec must be rejected at submit
	if _, err := e.Every(ctx, "never", "0 0 30 2 *"); err == nil {
		t.Fatal("Feb-30 spec accepted, want error (no occurrence within horizon)")
	}
}

func TestCronNormalEveryStillWorks(t *testing.T) {
	// guard: the stricter validation must not reject ordinary specs
	for _, spec := range []string{"*/5 * * * *", "0 9 * * *", "0 0 1 * *", "0 0 * * 1", "30 14 15 8 *"} {
		if _, err := parseCron(spec); err != nil {
			t.Errorf("parseCron(%q): %v", spec, err)
		}
	}
	cs, _ := parseCron("30 14 15 8 *")
	from := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	if want := time.Date(2027, 8, 15, 14, 30, 0, 0, time.UTC); !cs.nextAfter(from).Equal(want) {
		t.Fatalf("15 Aug 14:30 next = %v", cs.nextAfter(from))
	}
}
