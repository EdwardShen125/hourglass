// benchplot renders SVG charts from hourglass-bench artifacts
// (bench-results/*.json). Deterministic, dependency-free, and
// reproducible: the same artifact always renders the same chart.
//
// Usage:
//
//	benchplot -mode percentile a.json b.json -o docs/charts/latency.svg
//	benchplot -mode fire a.json -o docs/charts/fire-curve.svg
//
// percentile: submit→fire latency curve (p50 p90 p99 p99.9 p99.99 max)
// per artifact, one series each — tail shape at a glance.
// fire: per-second fire counts — the drain/storm curve.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
)

type artifact struct {
	Scenario    string           `json:"scenario"`
	Timestamp   string           `json:"timestamp"`
	Jobs        int              `json:"jobs"`
	Shards      int              `json:"shards"`
	FirePerSec  float64          `json:"fire_per_sec"`
	MaxMS       int64            `json:"max_ms"`
	FireCurve   []int            `json:"fire_curve"`
	Percentiles map[string]int64 `json:"percentiles"`
}

var palette = []string{
	"#2563eb", "#dc2626", "#059669", "#d97706", "#7c3aed", "#0891b2",
}

const (
	w, h   = 760, 420
	ml, mr = 64, 16
	mt, mb = 28, 46
)

func main() {
	mode := flag.String("mode", "percentile", "percentile | fire")
	out := flag.String("o", "", "output file (default stdout)")
	title := flag.String("title", "", "chart title (default from mode)")
	flag.Parse()
	if flag.NArg() == 0 {
		fmt.Fprintln(os.Stderr, "usage: benchplot [-mode percentile|fire] [-o out.svg] artifact.json...")
		os.Exit(2)
	}
	var arts []artifact
	for _, p := range flag.Args() {
		b, err := os.ReadFile(p)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		var a artifact
		if err := json.Unmarshal(b, &a); err != nil {
			fmt.Fprintf(os.Stderr, "%s: %v\n", p, err)
			os.Exit(1)
		}
		arts = append(arts, a)
	}
	t := *title
	if t == "" {
		if *mode == "fire" {
			t = "fires per second (drain curve)"
		} else {
			t = "submit→fire latency percentiles (ms)"
		}
	}
	var svg string
	switch *mode {
	case "fire":
		svg = fireChart(arts, t)
	default:
		svg = percentileChart(arts, t)
	}
	if *out == "" {
		fmt.Print(svg)
		return
	}
	if err := os.WriteFile(*out, []byte(svg), 0o644); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("wrote %s\n", *out)
}

type pt struct{ x, y float64 }

// frame draws the shared canvas: background, grid, axes, title, and
// returns the plot area bounds.
func frame(title string) (float64, float64, float64, float64, string) {
	pw, ph := float64(w-ml-mr), float64(h-mt-mb)
	var s string
	s += fmt.Sprintf("<svg xmlns='http://www.w3.org/2000/svg' width='%d' height='%d' viewBox='0 0 %d %d' font-family='ui-monospace,Menlo,monospace'>\n", w, h, w, h)
	s += fmt.Sprintf("<rect width='%d' height='%d' fill='#ffffff'/>\n", w, h)
	s += fmt.Sprintf("<text x='%d' y='18' font-size='13' fill='#111827' font-weight='bold'>%s</text>\n", ml, esc(title))
	s += fmt.Sprintf("<line x1='%d' y1='%d' x2='%d' y2='%d' stroke='#9ca3af'/>\n", ml, mt+int(ph), w-mr, mt+int(ph)) // x axis
	s += fmt.Sprintf("<line x1='%d' y1='%d' x2='%d' y2='%d' stroke='#9ca3af'/>\n", ml, mt, ml, mt+int(ph))           // y axis
	return float64(ml), float64(mt), pw, ph, s
}

// niceStep rounds a raw grid step to a 1/2/5×10^k multiple so axis
// ticks read as round numbers.
func niceStep(raw float64) float64 {
	if raw <= 0 {
		return 1
	}
	pow := math.Pow10(int(math.Floor(math.Log10(raw))))
	for _, m := range []float64{1, 2, 5, 10} {
		if raw <= m*pow {
			return m * pow
		}
	}
	return 10 * pow
}

// yGrid emits ~5 round horizontal gridlines scaled to maxV and
// returns the grid top (a round multiple ≥ maxV) so series can scale
// to the same ceiling.
func yGrid(s *string, x0, y0, pw, ph, maxV float64) (top float64) {
	step := niceStep(maxV / 5)
	top = math.Ceil(maxV/step) * step
	if top < step {
		top = step
	}
	for v := 0.0; v <= top+step/2; v += step {
		y := y0 + ph - ph*(v/top)
		*s += fmt.Sprintf("<line x1='%.0f' y1='%.0f' x2='%.0f' y2='%.0f' stroke='#e5e7eb'/>\n", x0, y, x0+pw, y)
		*s += fmt.Sprintf("<text x='%.0f' y='%.0f' font-size='10' fill='#6b7280' text-anchor='end'>%.0f</text>\n", x0-6, y+3, v)
	}
	return top
}

func percentileChart(arts []artifact, title string) string {
	labels := []string{"p50", "p90", "p99", "p99.9", "p99.99", "max"}
	keys := []string{"50", "90", "99", "99.9", "99.99"}
	x0, y0, pw, ph, s := frame(title)
	var maxY float64
	for _, a := range arts {
		for _, k := range keys {
			maxY = math.Max(maxY, float64(a.Percentiles[k]))
		}
		maxY = math.Max(maxY, float64(a.MaxMS))
	}
	if maxY <= 0 {
		maxY = 1
	}
	maxY *= 1.08
	top := yGrid(&s, x0, y0, pw, ph, maxY)
	stepX := pw / float64(len(labels)-1)
	for i, l := range labels {
		x := x0 + float64(i)*stepX
		s += fmt.Sprintf("<text x='%.0f' y='%.0f' font-size='10' fill='#374151' text-anchor='middle'>%s</text>\n", x, y0+ph+16, l)
	}
	// series
	for si, a := range arts {
		color := palette[si%len(palette)]
		pts := []pt{}
		for i, k := range keys {
			pts = append(pts, pt{x0 + float64(i)*stepX, y0 + ph - ph*(float64(a.Percentiles[k])/top)})
		}
		pts = append(pts, pt{x0 + float64(len(keys))*stepX, y0 + ph - ph*(float64(a.MaxMS)/top)})
		s += polyline(pts, color)
		for _, p := range pts {
			s += fmt.Sprintf("<circle cx='%.1f' cy='%.1f' r='2.5' fill='%s'/>\n", p.x, p.y, color)
		}
	}
	s += legend(arts, x0, y0+ph+30)
	s += "</svg>\n"
	return s
}

func fireChart(arts []artifact, title string) string {
	x0, y0, pw, ph, s := frame(title)
	maxT, maxY := 1.0, 1.0
	for _, a := range arts {
		maxT = math.Max(maxT, float64(len(a.FireCurve)))
		for _, v := range a.FireCurve {
			maxY = math.Max(maxY, float64(v))
		}
	}
	maxY *= 1.08
	topY := yGrid(&s, x0, y0, pw, ph, maxY)
	// x ticks: up to 8 labeled seconds
	ticks := min(8, int(maxT))
	if ticks < 2 {
		ticks = 2
	}
	for i := 0; i <= ticks; i++ {
		t := maxT * float64(i) / float64(ticks)
		x := x0 + pw*t/maxT
		s += fmt.Sprintf("<line x1='%.0f' y1='%.0f' x2='%.0f' y2='%.0f' stroke='#f3f4f6'/>\n", x, y0, x, y0+ph)
		s += fmt.Sprintf("<text x='%.0f' y='%.0f' font-size='10' fill='#374151' text-anchor='middle'>%.0fs</text>\n", x, y0+ph+16, t)
	}
	for si, a := range arts {
		color := palette[si%len(palette)]
		// Pad each series with zero buckets through maxT+1s so the
		// drain visibly returns to zero instead of clipping at the
		// last fire.
		pts := []pt{}
		for t := 0; t <= int(maxT)+1; t++ {
			v := 0
			if t < len(a.FireCurve) {
				v = a.FireCurve[t]
			}
			pts = append(pts, pt{x0 + pw*float64(t)/(maxT+1), y0 + ph - ph*(float64(v)/topY)})
		}
		s += polyline(pts, color)
	}
	s += fmt.Sprintf("<text x='%.0f' y='%.0f' font-size='10' fill='#6b7280' text-anchor='end'>x: seconds since scenario start</text>\n", x0+pw, y0+ph+30)
	s += legend(arts, x0, y0+ph+42)
	s += "</svg>\n"
	return s
}

func polyline(pts []pt, color string) string {
	s := fmt.Sprintf("<polyline fill='none' stroke='%s' stroke-width='1.8' points='", color)
	for i, p := range pts {
		if i > 0 {
			s += " "
		}
		s += fmt.Sprintf("%.1f,%.1f", p.x, p.y)
	}
	return s + "'/>\n"
}

func legend(arts []artifact, x, y float64) string {
	s := ""
	for i, a := range arts {
		lx := x + float64(i)*210
		if lx > float64(w-200) {
			break
		}
		color := palette[i%len(palette)]
		s += fmt.Sprintf("<line x1='%.0f' y1='%.0f' x2='%.0f' y2='%.0f' stroke='%s' stroke-width='3'/>\n", lx, y-4, lx+18, y-4, color)
		ts := a.Timestamp
		if len(ts) > 10 {
			ts = ts[:10]
		}
		s += fmt.Sprintf("<text x='%.0f' y='%.0f' font-size='10' fill='#374151'>%s n=%d sh=%d (%s)</text>\n", lx+24, y, esc(a.Scenario), a.Jobs, a.Shards, ts)
	}
	return s
}

func esc(s string) string {
	r := ""
	for _, c := range s {
		switch c {
		case '&':
			r += "&amp;"
		case '<':
			r += "&lt;"
		case '>':
			r += "&gt;"
		case '\'':
			r += "&apos;"
		default:
			r += string(c)
		}
	}
	return r
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
