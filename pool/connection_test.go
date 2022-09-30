package pool_test

import (
	"sync"
	"testing"
	"time"

	"context"

	"github.com/jxsl13/amqpx/pool"
	"github.com/stretchr/testify/assert"
)

func TestNewConnection(t *testing.T) {
	t.Parallel()

	var wg sync.WaitGroup

	connections := 200
	wg.Add(connections)
	for i := 0; i < connections; i++ {
		go func(id int64) {
			defer wg.Done()

			c, err := pool.NewConnection("amqp://admin:password@localhost:5672", "TestNewConnection", id)
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

func TestNewConnectionContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	c, err := pool.NewConnection(
		"amqp://admin:password@localhost:5672",
		"TestNewConnectionContext",
		0,
		pool.ConnectionWithContext(ctx),
	)
	assert.NoError(t, err)
	time.Sleep(3 * time.Second)
	cancel()
	time.Sleep(3 * time.Second)
	assert.ErrorIs(t, c.Error(), pool.ErrConnectionClosed)
}
