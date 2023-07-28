package pool_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jxsl13/amqpx/logging"
	"github.com/jxsl13/amqpx/pool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewSingleConnection(t *testing.T) {
	c, err := pool.NewConnection(
		"amqp://admin:password@localhost:5672",
		"TestNewSingleConnection",
		pool.ConnectionWithLogger(logging.NewTestLogger(t)),
		pool.ConnectionWithSlowClose(true),
	)

	if err != nil {
		require.NoError(t, err)
		return
	}
	defer func() {
		err := c.Close()
		require.NoError(t, err)
	}()
}

func TestNewSingleConnectionWithDisconnect(t *testing.T) {
	started, stopped := DisconnectWithStartedStopped(t, 0, 0, 15*time.Second)
	started()
	defer stopped()
	c, err := pool.NewConnection(
		"amqp://admin:password@localhost:5672",
		"TestNewSingleConnection",
		pool.ConnectionWithLogger(logging.NewTestLogger(t)),
		pool.ConnectionWithSlowClose(true),
	)

	if err != nil {
		require.NoError(t, err)
		return
	}
	defer func() {
		err := c.Close()
		require.NoError(t, err)
	}()
}

func TestNewConnection(t *testing.T) {
	var wg sync.WaitGroup

	connections := 5
	wg.Add(connections)
	for i := 0; i < connections; i++ {
		go func(id int64) {
			defer wg.Done()

			c, err := pool.NewConnection(
				"amqp://admin:password@localhost:5672",
				fmt.Sprintf("TestNewConnection-%d", id),
				pool.ConnectionWithLogger(logging.NewTestLogger(t)),
				pool.ConnectionWithSlowClose(true),
			)
			if err != nil {
				assert.NoError(t, err)
				return
			}
			defer func() {
				assert.Error(t, c.Error())
			}()
			defer c.Close()
			time.Sleep(2 * time.Second)
			assert.NoError(t, c.Error())
		}(int64(i))
	}

	wg.Wait()
}

func TestNewConnectionDisconnect(t *testing.T) {

	var wg sync.WaitGroup

	connections := 100
	wg.Add(connections)

	// disconnect directly for 10 seconds
	wait := DisconnectWithStopped(t, 0, 0, time.Second)
	defer wait() // wait for goroutine to properly close & unblock the proxy

	for i := 0; i < connections; i++ {
		go func(id int64) {
			defer wg.Done()

			c, err := pool.NewConnection(
				"amqp://admin:password@localhost:5672",
				fmt.Sprintf("TestNewConnectionDisconnect-%d", id),
				//pool.ConnectionWithLogger(logging.NewTestLogger(t)),
				pool.ConnectionWithSlowClose(true),
			)
			if err != nil {
				assert.NoError(t, err)
				return
			}
			defer func() {
				assert.Error(t, c.Error())
			}()
			defer c.Close()

			wait() // wait for connection to work again.

			assert.NoError(t, c.Recover())
			assert.NoError(t, c.Error())
		}(int64(i))
	}

	wg.Wait()
}
