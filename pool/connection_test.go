package pool_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jxsl13/amqpx/pool"
	"github.com/stretchr/testify/assert"
)

func TestNewConnection(t *testing.T) {
	var wg sync.WaitGroup

	connections := 200
	wg.Add(connections)
	for i := 0; i < connections; i++ {
		go func(id int64) {
			defer wg.Done()

			c, err := pool.NewConnection("amqp://admin:password@localhost:5672", fmt.Sprintf("TestNewConnection-%d", id))
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
