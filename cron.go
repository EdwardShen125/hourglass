package hourglass

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// cronSpec is a parsed standard 5-field cron expression:
// minute hour dom month dow. dom and dow OR when both are restricted
// (V7 semantics).
type cronSpec struct {
	minute, hour, dom, month, dow uint64 // bitmask: bit i set = value i allowed
	domStar, dowStar              bool
}

func bit(v int) uint64 { return 1 << uint(v) }

// parseCron parses a standard 5-field cron expression. Steps (*/n),
// ranges (a-b), lists (a,b,c) supported; names not (use numbers).
func parseCron(spec string) (*cronSpec, error) {
	fields := strings.Fields(spec)
	if len(fields) != 5 {
		return nil, fmt.Errorf("hourglass: cron %q: want 5 fields, got %d", spec, len(fields))
	}
	ranges := []struct {
		lo, hi int
	}{
		{0, 59}, {0, 23}, {1, 31}, {1, 12}, {0, 6},
	}
	out := &cronSpec{}
	for i, f := range fields {
		r := ranges[i]
		mask, star, err := parseField(f, r.lo, r.hi)
		if err != nil {
			return nil, fmt.Errorf("hourglass: cron %q field %d (%s): %w", spec, i+1, f, err)
		}
		switch i {
		case 0:
			out.minute = mask
		case 1:
			out.hour = mask
		case 2:
			out.dom, out.domStar = mask, star
		case 3:
			out.month = mask
		case 4:
			out.dow, out.dowStar = mask, star
		}
	}
	return out, nil
}

func parseField(f string, lo, hi int) (mask uint64, star bool, err error) {
	if f == "*" {
		return fullMask(lo, hi), true, nil
	}
	for _, part := range strings.Split(f, ",") {
		step := 1
		if idx := strings.IndexByte(part, '/'); idx >= 0 {
			s, cerr := strconv.Atoi(part[idx+1:])
			if cerr != nil || s <= 0 {
				return 0, false, fmt.Errorf("bad step %q", part[idx+1:])
			}
			step = s
			part = part[:idx]
		}
		b, e := lo, hi
		switch {
		case part == "*":
			// */n
		case strings.ContainsRune(part, '-'):
			parts := strings.SplitN(part, "-", 2)
			b, err = strconv.Atoi(parts[0])
			if err != nil {
				return 0, false, err
			}
			e, err = strconv.Atoi(parts[1])
			if err != nil {
				return 0, false, err
			}
		default:
			b, err = strconv.Atoi(part)
			if err != nil {
				return 0, false, fmt.Errorf("bad number %q", part)
			}
			if strings.ContainsRune(f, '/') {
				e = hi // "5/15" = 5,20,35,50
			} else {
				e = b
			}
		}
		if b < lo || e > hi || b > e {
			return 0, false, fmt.Errorf("value out of range [%d,%d]", lo, hi)
		}
		for v := b; v <= e; v += step {
			mask |= bit(v)
		}
	}
	return mask, false, nil
}

func fullMask(lo, hi int) uint64 {
	var m uint64
	for v := lo; v <= hi; v++ {
		m |= bit(v)
	}
	return m
}

// nextAfter returns the next matching time strictly after t, second
// zeroed, evaluated in t's location.
func (c *cronSpec) nextAfter(t time.Time) time.Time {
	// start at the next whole minute strictly after t
	n := t.Truncate(time.Minute).Add(time.Minute)
	limit := n.AddDate(1, 0, 0) // sanity bound: 1 year of search
	for n.Before(limit) {
		if !c.monthMatches(n) {
			n = firstOfNextMonth(n)
			continue
		}
		if !c.dayMatches(n) {
			n = nextMidnight(n)
			continue
		}
		if !c.hourMatches(n) {
			n = nextHour(n)
			continue
		}
		if !c.minuteMatches(n) {
			n = n.Add(time.Minute)
			continue
		}
		return n
	}
	return time.Time{} // unreachable for any real spec
}

func (c *cronSpec) monthMatches(t time.Time) bool { return c.month&bit(int(t.Month())) != 0 }

func (c *cronSpec) dayMatches(t time.Time) bool {
	domOK := c.dom&bit(t.Day()) != 0
	dowOK := c.dow&bit(int(t.Weekday())) != 0
	// V7: both restricted -> OR; either unrestricted -> the restricted one
	if !c.domStar && !c.dowStar {
		return domOK || dowOK
	}
	if c.domStar && c.dowStar {
		return true
	}
	if c.domStar {
		return dowOK
	}
	return domOK
}

func (c *cronSpec) hourMatches(t time.Time) bool   { return c.hour&bit(t.Hour()) != 0 }
func (c *cronSpec) minuteMatches(t time.Time) bool { return c.minute&bit(t.Minute()) != 0 }

func firstOfNextMonth(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, t.Location()).AddDate(0, 1, 0)
}

func nextMidnight(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location()).AddDate(0, 0, 1)
}

func nextHour(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location()).Add(time.Hour)
}
