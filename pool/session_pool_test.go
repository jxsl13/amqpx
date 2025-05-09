package pool_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jxsl13/amqpx/internal/testlogger"
	"github.com/jxsl13/amqpx/internal/testutils"
	"github.com/jxsl13/amqpx/pool"
	"github.com/stretchr/testify/assert"
)

func TestSingleSessionPool(t *testing.T) {
	t.Parallel()
	var (
		poolName    = testutils.FuncName()
		ctx         = context.TODO()
		connections = 1
		sessions    = 1
	)
	p, err := pool.NewConnectionPool(ctx,
		testutils.HealthyConnectURL,
		connections,
		pool.ConnectionPoolWithName(poolName),
		pool.ConnectionPoolWithLogger(testlogger.NewTestLogger(t)),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}

	sp, err := pool.NewSessionPool(
		p,
		sessions,
		pool.SessionPoolWithAutoCloseConnectionPool(true),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer sp.Close()

	s, err := sp.GetSession(ctx)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	time.Sleep(testutils.Jitter(1*time.Second, 3*time.Second))

	assert.NoError(t, s.Recover(ctx))

	sp.ReturnSession(s, nil)
}

func TestNewSessionPool(t *testing.T) {
	t.Parallel()
	var (
		poolName    = testutils.FuncName()
		ctx         = context.TODO()
		connections = 1
		sessions    = 100
	)
	p, err := pool.NewConnectionPool(ctx,
		testutils.HealthyConnectURL,
		connections,
		pool.ConnectionPoolWithName(poolName),
		pool.ConnectionPoolWithLogger(testlogger.NewTestLogger(t)),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer p.Close()

	sp, err := pool.NewSessionPool(
		p,
		sessions,
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer sp.Close()

	var wg sync.WaitGroup

	for i := 0; i < sessions*2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, err := sp.GetSession(ctx)
			if err != nil {
				assert.NoError(t, err)
				return
			}
			time.Sleep(testutils.Jitter(10*time.Millisecond, 30*time.Millisecond))
			sp.ReturnSession(s, nil)
		}()
	}

	wg.Wait()
}
