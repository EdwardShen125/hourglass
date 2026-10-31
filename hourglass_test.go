package hourglass_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/EdwardShen125/hourglass"
)

func openTestDB(t *testing.T) *hourglass.Handle {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "hourglass.db")
	h, err := hourglass.Open("sqlite://" + dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { h.Close() })
	return h
}

func TestOpenCreatesSchema(t *testing.T) {
	h := openTestDB(t)
	if err := h.Ping(context.Background()); err != nil {
		t.Fatalf("Ping after Open: %v", err)
	}
}

func TestSubmitPersistsJob(t *testing.T) {
	h := openTestDB(t)
	ctx := context.Background()

	due := time.Date(2026, 11, 1, 9, 0, 0, 0, time.UTC)
	id, err := h.Submit(ctx, "email:welcome", []byte(`{"uid":42}`), hourglass.At(due))
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if id == "" {
		t.Fatal("Submit returned empty id")
	}

	j, err := h.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if j.Topic != "email:welcome" {
		t.Errorf("Topic = %q, want email:welcome", j.Topic)
	}
	if !j.Due.Equal(due) {
		t.Errorf("Due = %v, want %v", j.Due, due)
	}
	if j.State != hourglass.Waiting {
		t.Errorf("State = %v, want waiting", j.State)
	}
	if string(j.Payload) != `{"uid":42}` {
		t.Errorf("Payload = %q", j.Payload)
	}
}
