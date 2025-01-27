package types

import (
	"math/rand/v2"
	"time"
)

type BackoffFunc func(retry int) (sleep time.Duration)

// NewBackoffPolicy creates a new backoff policy with the given min and max duration.
func NewBackoffPolicy(minDuration, maxDuration time.Duration) BackoffFunc {

	factor := time.Second
	for _, scale := range []time.Duration{time.Hour, time.Minute, time.Second, time.Millisecond, time.Microsecond, time.Nanosecond} {
		d := minDuration.Truncate(scale)
		if d > 0 {
			factor = scale
			break
		}
	}

	return func(retry int) (sleep time.Duration) {

		wait := 2 << max(0, min(32, retry)) * factor                     // 2^(min(32, retry)) * factor (second, min, hours, etc)
		jitter := time.Duration(rand.Int64N(int64(max(1, int(wait)/5)))) // max 20% jitter
		wait = minDuration + wait + jitter
		if wait > maxDuration {
			wait = maxDuration
		}
		return wait
	}
}
