package pool

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jxsl13/amqpx/logging"
	"github.com/stretchr/testify/assert"
)

func worker(t *testing.T, ctx context.Context, sc *stateContext) {
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
			log.Debug("worker done")
			return
		case <-sc.Resuming().Done():
			log.Debug("worker resuming")
			sc.Resumed()
			log.Debug("worker resumed")
		}
		select {
		case <-ctx.Done():
			return
		case <-sc.Pausing().Done():
			log.Debug("worker pausing")
			sc.Paused()
			log.Debug("worker paused")
		}
	}
}

func TestStateContextSimpleSynchronized(t *testing.T) {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer cancel()

	sc := newStateContext(ctx)
	go worker(t, ctx, sc)

	// normal execution order
	assert.NoError(t, sc.Resume(ctx))
	assert.NoError(t, sc.Pause(ctx))

	// somewhat random execution order
	assert.NoError(t, sc.Pause(ctx)) // if already paused, nobody cares
	assert.NoError(t, sc.Resume(ctx))

	assert.NoError(t, sc.Resume(ctx)) // should be ignored
	assert.NoError(t, sc.Pause(ctx))
}

func TestStateContextConcurrentTransitions(t *testing.T) {
	log := logging.NewTestLogger(t)
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer cancel()

	sc := newStateContext(ctx)
	go worker(t, ctx, sc)

	var wg sync.WaitGroup
	numGoroutines := 100
	trigger := make(chan struct{})

	var (
		pause  atomic.Int64
		resume atomic.Int64
	)
	wg.Add(numGoroutines / 2)
	for i := 0; i < numGoroutines/2; i++ {
		go func(id int) {
			defer wg.Done()
			select {
			case <-ctx.Done():
				return
			case <-trigger:
				log.Infof("routine %d triggered", id)
			}

			for i := 0; i < 1000; i++ {
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

	wg.Add(numGoroutines / 2)
	for i := 0; i < numGoroutines/2; i++ {
		go func(id int) {
			defer wg.Done()
			select {
			case <-ctx.Done():
				return
			case <-trigger:
				log.Infof("routine %d triggered", id)
			}

			for i := 0; i < 1000; i++ {
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

	log.Debugf("pause: %d", pause.Load())
	log.Debugf("resume: %d", resume.Load())
	log.Debugf("active: %d", active.Load())
	log.Debugf("awaitPaused: %d", awaitPaused.Load())
	log.Debugf("awaitResumed: %d", awaitResumed.Load())
}
