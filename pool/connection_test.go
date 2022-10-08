package pool_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jxsl13/amqpx/logging"
	"github.com/jxsl13/amqpx/pool"
	"github.com/stretchr/testify/assert"
)

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

	connections := 100 // don't go below 100
	wg.Add(connections)

	// disconnect in a second for a second
	wait := DisconnectWithStop(t, 0, 10*time.Millisecond*time.Duration(connections), 5*time.Second)
	defer wait() // wait for goroutine to properly close & unblock the proxy

	for i := 0; i < connections; i++ {
		go func(id int64) {
			defer wg.Done()

			c, err := pool.NewConnection(
				"amqp://admin:password@localhost:5672",
				fmt.Sprintf("TestNewConnectionDisconnect-%d", id),
				pool.ConnectionWithLogger(logging.NewTestLogger(t)),
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
