package types_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jxsl13/amqpx/internal/proxyutils"
	"github.com/jxsl13/amqpx/internal/testlogger"
	"github.com/jxsl13/amqpx/internal/testutils"
	"github.com/jxsl13/amqpx/types"
	"github.com/stretchr/testify/assert"
)

func TestNewSingleConnection(t *testing.T) {
	t.Parallel()
	var (
		ctx      = context.TODO()
		nextName = testutils.ConnectionNameGenerator()
	)

	c, err := types.NewConnection(
		ctx,
		testutils.HealthyConnectURL,
		nextName(),
		types.ConnectionWithLogger(testlogger.NewTestLogger(t)),
	)

	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer func() {
		assert.NoError(t, c.Close())
	}()
}

func TestManyNewConnection(t *testing.T) {
	t.Parallel()
	var (
		ctx         = context.TODO()
		wg          sync.WaitGroup
		connections = 5
		nextName    = testutils.ConnectionNameGenerator()
	)

	wg.Add(connections)
	for i := 0; i < connections; i++ {
		go func() {
			defer wg.Done()

			c, err := types.NewConnection(
				ctx,
				testutils.HealthyConnectURL,
				nextName(),
				types.ConnectionWithLogger(testlogger.NewTestLogger(t)),
			)
			if err != nil {
				assert.NoError(t, err)
				return
			}
			defer func() {
				// error closed
				assert.Error(t, c.Error())
			}()
			defer c.Close()
			time.Sleep(testutils.Jitter(time.Second, 3*time.Second))
			assert.NoError(t, c.Error())
		}()
	}

	wg.Wait()
}

func TestNewSingleConnectionWithDisconnect(t *testing.T) {
	t.Parallel()
	var (
		ctx                      = context.TODO()
		proxyName, connectURL, _ = testutils.NextConnectURL()
		nextName                 = testutils.ConnectionNameGenerator()
	)

	started, stopped := proxyutils.DisconnectWithStartedStopped(t, proxyName, 0, 0, 10*time.Second)
	started()
	defer stopped()

	c, err := types.NewConnection(
		ctx,
		connectURL,
		nextName(),
		types.ConnectionWithLogger(testlogger.NewTestLogger(t)),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer func() {
		assert.NoError(t, c.Close())
	}()
}

func TestManyNewConnectionWithDisconnect(t *testing.T) {

	var (
		ctx                      = context.TODO()
		proxyName, connectURL, _ = testutils.NextConnectURL()
	)
	var (
		wg          sync.WaitGroup
		connections = 100
		nextName    = testutils.ConnectionNameGenerator()
	)
	wait := proxyutils.DisconnectWithStopped(t, proxyName, 0, 0, time.Second)
	defer wait() // wait for goroutine to properly close & unblock the proxy

	wg.Add(connections)
	for i := 0; i < connections; i++ {
		go func() {
			defer wg.Done()

			c, err := types.NewConnection(
				ctx,
				connectURL,
				nextName(),
			)
			if err != nil {
				assert.NoError(t, err)
				return
			}
			defer func() {
				// err closed
				assert.Error(t, c.Error())
			}()
			defer c.Close()

			wait() // wait for connection to work again.

			tctx, cancel := context.WithTimeout(ctx, 20*time.Second)
			defer cancel()
			assert.NoError(t, c.Recover(tctx))
			assert.NoError(t, c.Error())
		}()
	}

	wg.Wait()
}
