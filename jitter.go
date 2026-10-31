package hourglass

import (
	"math/rand"
	"time"
)

// drawJitter returns rand(0, w], seeded deterministically in tests.
func drawJitter(w time.Duration, seed int64) time.Duration {
	if w <= 0 {
		return 0
	}
	r := rand.New(rand.NewSource(seed))
	return time.Duration(1 + r.Int63n(int64(w)))
}
