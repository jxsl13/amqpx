package pool

import "time"

// closeTimer should be used as a deferred function
// in order to cleanly shut down a timer
func closeTimer(timer *time.Timer) {
	if !timer.Stop() {
		<-timer.C
	}
}

func resetTimer(timer *time.Timer, duration time.Duration) {
	if !timer.Stop() {
		<-timer.C
	}
	timer.Reset(duration)
}
