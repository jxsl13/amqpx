package pool

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBackoffPolicy(t *testing.T) {
	t.Parallel()

	backoffMultiTest(t, 15, 3)
	backoffMultiTest(t, 32, 4)
	backoffMultiTest(t, 64, 5)
}

func backoffMultiTest(t *testing.T, factor int, iterations int) {
	backoffTest(t, time.Hour, time.Duration(factor)*time.Hour, iterations)
	backoffTest(t, time.Minute, time.Duration(factor)*time.Minute, iterations)
	backoffTest(t, time.Second, time.Duration(factor)*time.Second, iterations)
	backoffTest(t, time.Millisecond, time.Duration(factor)*time.Millisecond, iterations)
	backoffTest(t, time.Nanosecond, time.Duration(factor)*time.Nanosecond, iterations)
}

func backoffTest(t *testing.T, min, max time.Duration, expectedInterations int) {
	for i := 0; i < 5000; i++ {
		backoff := newDefaultBackoffPolicy(min, max)

		previous := time.Duration(0)
		iterations := 0
		for i := 0; i < 128; i++ {
			sleep := backoff(i)
			if sleep == max {
				break
			}
			require.Less(t, previous, sleep)
			require.LessOrEqual(t, sleep, max)
			previous = sleep
			iterations++
		}

		require.Equalf(t, expectedInterations, iterations, "expected that many iterations: min: %v max: %v", min, max)
	}
}
