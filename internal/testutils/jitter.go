package testutils

import (
	"math/rand"
	"time"
)

func Jitter(min, max time.Duration) time.Duration {
	return min + time.Duration(rand.Int63n(int64(max-min)))
}
