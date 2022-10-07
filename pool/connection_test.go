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
			time.Sleep(5 * time.Second)
			assert.NoError(t, c.Error())
		}(int64(i))
	}

	wg.Wait()
}

func TestNewConnectionDisconnect(t *testing.T) {

	c, err := pool.NewConnection(
		"amqp://admin:password@localhost:5672",
		"TestNewConnection",
		pool.ConnectionWithLogger(logging.NewTestLogger(t)),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer c.Close()

	Disconnect(t, time.Second, 0, 2*time.Second)

	assert.NoError(t, c.Recover())
	assert.NoError(t, c.Error())
}
