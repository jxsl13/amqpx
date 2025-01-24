package testutils

import (
	"math/rand/v2"
	"time"
)

func Jitter(min, max time.Duration) time.Duration {
	diff := int64(max - min)
	if diff < 0 {
		diff = -diff
		min = max
	}

	return min + time.Duration(rand.Int64N(diff))
}
