package pool

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jxsl13/amqpx/logging"
	"github.com/stretchr/testify/assert"
)

func TestStateContextSimpleSynchronized(t *testing.T) {
	t.Parallel()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer cancel()

	sc := newStateContext(ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	go worker(t, ctx, &wg, sc)

	// normal execution order
	assert.NoError(t, sc.Resume(ctx))
	assert.NoError(t, sc.Pause(ctx))

	// somewhat random execution order
	assert.NoError(t, sc.Pause(ctx)) // if already paused, nobody cares
	assert.NoError(t, sc.Resume(ctx))

	assert.NoError(t, sc.Resume(ctx)) // should be ignored
	assert.NoError(t, sc.Pause(ctx))

	cancel()
	wg.Wait()
}

func TestStateContextConcurrentTransitions(t *testing.T) {
	t.Parallel()

	log := logging.NewTestLogger(t)
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer cancel()

	sc := newStateContext(ctx)
	var wwg sync.WaitGroup
	wwg.Add(1)
	go worker(t, ctx, &wwg, sc)

	var wg sync.WaitGroup
	var (
		numGoroutines = 2000
		iterations    = 10000
	)
	trigger := make(chan struct{})

	var (
		pause  atomic.Int64
		resume atomic.Int64
	)
	wg.Add(numGoroutines)
	for i := 0; i < (numGoroutines / 10 * 8); i++ {
		go func(id int) {
			defer wg.Done()
			select {
			case <-ctx.Done():
				return
			case <-trigger:
				//log.Infof("routine %d triggered", id)
			}
			time.Sleep(time.Duration(id/100) * 20 * time.Millisecond)

			for i := 0; i < iterations; i++ {
				if id%2 == 0 {
					assert.NoError(t, sc.Resume(ctx))
					// log.Infof("routine %d resumed", id)
					pause.Add(1)
				} else {
					assert.NoError(t, sc.Pause(ctx))
					// log.Infof("routine %d paused", id)
					resume.Add(1)
				}
			}
		}(i)
	}

	var (
		active       atomic.Int64
		awaitPaused  atomic.Int64
		awaitResumed atomic.Int64
	)

	for i := 0; i < (numGoroutines / 10 * 2); i++ {
		go func(id int) {
			defer wg.Done()
			select {
			case <-ctx.Done():
				return
			case <-trigger:
				//log.Infof("routine %d triggered", id)
			}
			time.Sleep(time.Duration(id/100) * 10 * time.Millisecond)

			for i := 0; i < iterations; i++ {
				switch id % 3 {
				case 0:
					_, err := sc.IsActive(ctx)
					assert.NoError(t, err)
					active.Add(1)
				case 1:
					assert.NoError(t, sc.AwaitPaused(ctx))
					// log.Infof("routine %d await paused", id)
					awaitPaused.Add(1)
				case 2:
					assert.NoError(t, sc.AwaitResumed(ctx))
					// log.Infof("routine %d await resumed", id)
					awaitResumed.Add(1)
				}
			}

		}(numGoroutines/2 + i)
	}

	close(trigger)
	wg.Wait()

	cancel()
	wwg.Wait()

	log.Debugf("pause: %d", pause.Load())
	log.Debugf("resume: %d", resume.Load())
	log.Debugf("active: %d", active.Load())
	log.Debugf("awaitPaused: %d", awaitPaused.Load())
	log.Debugf("awaitResumed: %d", awaitResumed.Load())
}

func worker(t *testing.T, ctx context.Context, wg *sync.WaitGroup, sc *stateContext) {
	defer wg.Done()

	log := logging.NewTestLogger(t)
	defer func() {
		log.Debug("worker pausing (closing)")
		sc.Paused()
		log.Debug("worker paused (closing)")
		log.Debug("worker closed")
	}()
	log.Debug("worker started")

	for {
		select {
		case <-ctx.Done():
			//log.Debug("worker done")
			return
		case <-sc.Resuming().Done():
			//log.Debug("worker resuming")
			sc.Resumed()
			go func() {
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Second):
					sc.Pause(ctx) // always have at least one goroutine that triggers the switch back to the other state after a specific time
				}
			}()
			//log.Debug("worker resumed")
		}
		select {
		case <-ctx.Done():
			return
		case <-sc.Pausing().Done():
			//log.Debug("worker pausing")
			sc.Paused()
			//log.Debug("worker paused")
			go func() {
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Second):
					sc.Resume(ctx) // always have at least one goroutine that triggers the switch back to the other state after a specific time
				}
			}()
		}
	}
}
