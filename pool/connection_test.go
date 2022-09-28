package pool_test

import (
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/jxsl13/amqpx/pool"
	"github.com/stretchr/testify/assert"
)

func TestNewConnection(t *testing.T) {
	t.Parallel()

	rand.Seed(time.Now().Unix())
	var wg sync.WaitGroup

	for i := int64(0); i < 200; i++ {
		wg.Add(1)
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
			time.Sleep(15 * time.Second)
			assert.NoError(t, c.Error())
		}(i)
	}

	wg.Wait()
}
