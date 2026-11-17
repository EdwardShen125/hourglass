package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
)

// latTracker records submit→fire latency per job into an HDR histogram
// (2 significant digits, 1ms floor, 1h ceiling; values stored in ms).
// The per-id map makes each measurement source-anchored: the delta is
// taken from the job's own submit timestamp, not from a sampling
// thread, so load bursts cannot hide tail latency (no coordinated
// omission from the measuring side).
type latTracker struct {
	submitted sync.Map // job id -> time.Time
	mu        sync.Mutex
	h         *hdrhistogram.Histogram
}

func newLatTracker() *latTracker {
	return &latTracker{
		h: hdrhistogram.New(1, int64(time.Hour/time.Millisecond), 2),
	}
}

func (t *latTracker) markSubmit(id string, at time.Time) {
	t.submitted.Store(id, at)
}

func (t *latTracker) markFired(id string, at time.Time) {
	v, ok := t.submitted.LoadAndDelete(id)
	if !ok {
		return
	}
	ms := int64(at.Sub(v.(time.Time)) / time.Millisecond)
	if ms < 1 {
		ms = 1 // histogram floor
	}
	t.mu.Lock()
	_ = t.h.RecordValue(ms)
	t.mu.Unlock()
}

var benchPercentiles = []float64{50, 90, 99, 99.9, 99.99}

// hdrFields renders the percentile table for emit().
func (t *latTracker) hdrFields(prefix string) [][2]string {
	t.mu.Lock()
	defer t.mu.Unlock()
	fields := [][2]string{
		{prefix + "count", fmt.Sprintf("%d", t.h.TotalCount())},
	}
	for _, p := range benchPercentiles {
		fields = append(fields, [2]string{
			fmt.Sprintf("%sp%s", prefix, trimPct(p)),
			fmtMs(float64(t.h.ValueAtPercentile(p))),
		})
	}
	fields = append(fields, [2]string{prefix + "max", fmtMs(float64(t.h.Max()))})
	return fields
}

func trimPct(p float64) string {
	if p == float64(int64(p)) {
		return fmt.Sprintf("%d", int64(p))
	}
	s := fmt.Sprintf("%.2f", p) // 99.90 -> 99.9
	for len(s) > 0 && s[len(s)-1] == '0' {
		s = s[:len(s)-1]
	}
	if len(s) > 0 && s[len(s)-1] == '.' {
		s = s[:len(s)-1]
	}
	return s
}

// benchstatLines renders go-test-shaped lines that `benchstat` parses
// directly, so scenario runs can be diffed statistically:
//
//	benchstat old.txt new.txt
//
// N is the job count; ns/op is the per-job time for submit and for
// fire respectively. Percentiles live in the human table and the JSON
// artifact; benchstat carries the throughput pair.
func (t *latTracker) benchstatLines(name string, n int, submitDur, drainDur time.Duration, fired int64) []string {
	return []string{
		fmt.Sprintf("Benchmark%sSubmit/%s %d %.0f ns/op", name, name, n, float64(submitDur.Nanoseconds())/float64(n)),
		fmt.Sprintf("Benchmark%sFire/%s %d %.0f ns/op", name, name, fired, float64(drainDur.Nanoseconds())/float64(max(fired, 1))),
	}
}

// writeArtifact archives the run for later re-quantiling and diffing.
// Layout: bench-results/<scenario>-<timestamp>.json
func (t *latTracker) writeArtifact(scenario string, extra map[string]any) (string, error) {
	t.mu.Lock()
	hdrExport := t.h.Export()
	t.mu.Unlock()
	dir := "bench-results"
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	doc := map[string]any{
		"scenario":    scenario,
		"timestamp":   time.Now().UTC().Format(time.RFC3339),
		"goos":        runtime.GOOS,
		"goarch":      runtime.GOARCH,
		"cpus":        runtime.NumCPU(),
		"hdr_unit":    "ms",
		"hdr":         hdrExport,
		"percentiles": map[string]int64{},
	}
	for _, p := range benchPercentiles {
		doc["percentiles"].(map[string]int64)[trimPct(p)] = t.h.ValueAtPercentile(p)
	}
	for k, v := range extra {
		doc[k] = v
	}
	b, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return "", err
	}
	path := filepath.Join(dir, fmt.Sprintf("%s-%s.json", scenario, time.Now().UTC().Format("20060102T150405Z")))
	if err := os.WriteFile(path, b, 0o644); err != nil {
		return "", err
	}
	return path, nil
}
