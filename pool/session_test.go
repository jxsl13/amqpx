package pool_test

import (
	"sync"
	"testing"
	"time"

	"github.com/jxsl13/amqpx/pool"
	"github.com/stretchr/testify/assert"
)

func TestNewSession(t *testing.T) {
	t.Parallel()

	c, err := pool.NewConnection("amqp://admin:password@localhost:5672", "TestNewSession", 1)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer c.Close()

	var wg sync.WaitGroup

	for id := 0; id < 200; id++ {
		wg.Add(1)
		go func(id int64) {
			defer wg.Done()
			s, err := pool.NewSession(c, id, false)
			if err != nil {
				assert.NoError(t, err)
				return
			}
			defer s.Close()

			time.Sleep(15 * time.Second)
		}(int64(id))
	}

	wg.Wait()
}
