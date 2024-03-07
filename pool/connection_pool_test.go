package pool_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jxsl13/amqpx/internal/testutils"
	"github.com/jxsl13/amqpx/logging"
	"github.com/jxsl13/amqpx/pool"
	"github.com/stretchr/testify/assert"
)

func TestNewSingleConnectionPool(t *testing.T) {
	t.Parallel() // can be run in parallel because the connection to the rabbitmq is never broken

	poolName := testutils.FuncName()
	ctx := context.TODO()

	connections := 1
	p, err := pool.NewConnectionPool(
		ctx,
		testutils.HealthyConnectURL,
		connections,
		pool.ConnectionPoolWithName(poolName),
		pool.ConnectionPoolWithLogger(logging.NewTestLogger(t)),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer p.Close()

	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	c, err := p.GetConnection(cctx)
	if err != nil {
		assert.NoError(t, err)
		return
	}

	assert.NoError(t, c.Recover(ctx)) // should not need to recover

	time.Sleep(testutils.Jitter(1*time.Second, 5*time.Second))
	p.ReturnConnection(c, nil)

}

func TestNewConnectionPool(t *testing.T) {
	t.Parallel() // can be run in parallel because the connection to the rabbitmq is never broken

	poolName := testutils.FuncName()

	ctx := context.TODO()
	connections := 5
	p, err := pool.NewConnectionPool(ctx,
		testutils.HealthyConnectURL,
		connections,
		pool.ConnectionPoolWithName(poolName),
		pool.ConnectionPoolWithLogger(logging.NewTestLogger(t)),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer p.Close()
	var wg sync.WaitGroup

	wg.Add(connections)
	for i := 0; i < connections; i++ {
		go func(i int) {
			defer wg.Done()

			cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()

			c, err := p.GetConnection(cctx)
			if err != nil {
				assert.NoError(t, err)
				return
			}

			// should not need to recover
			assert.NoError(t, c.Recover(ctx))

			time.Sleep(testutils.Jitter(1*time.Second, 5*time.Second))
			p.ReturnConnection(c, nil)
		}(i)
	}

	wg.Wait()
}

func TestNewConnectionPoolWithDisconnect(t *testing.T) {
	var (
		ctx                      = context.TODO()
		poolName                 = testutils.FuncName()
		proxyName, connectURL, _ = testutils.NextConnectURL()
	)

	connections := 100
	p, err := pool.NewConnectionPool(
		ctx,
		connectURL,
		connections,
		pool.ConnectionPoolWithName(poolName),
		pool.ConnectionPoolWithLogger(logging.NewTestLogger(t)),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer p.Close()
	var wg sync.WaitGroup

	disconnectDuration := 5 * time.Second

	awaitStarted, awaitStopped := DisconnectWithStartedStopped(
		t,
		proxyName,
		0,
		5*time.Second,
		disconnectDuration,
	)
	defer awaitStopped()

	for i := 0; i < connections; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			awaitStarted() // wait for connection loss

			// no connection, this should retry until there is a connection
			c, err := p.GetConnection(ctx)
			if err != nil {
				assert.NoError(t, err)
				return
			}
			maxSleep := disconnectDuration - time.Second
			sleep := testutils.Jitter(maxSleep/2, maxSleep)

			time.Sleep(sleep)
			cctx, cancel := context.WithTimeout(ctx, disconnectDuration)
			defer cancel()
			assert.NoError(t, c.Recover(cctx))

			p.ReturnConnection(c, nil)
		}(i)
	}

	wg.Wait()
}
