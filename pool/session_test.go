package pool_test

import (
	"sync"
	"testing"
	"time"

	"github.com/jxsl13/amqpx/pool"
	"github.com/stretchr/testify/assert"
)

func TestNewSession(t *testing.T) {

	c, err := pool.NewConnection("amqp://admin:password@localhost:5672", "TestNewSession", 1)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer c.Close()

	var wg sync.WaitGroup

	sessions := 200
	wg.Add(sessions)
	for id := 0; id < sessions; id++ {
		go func(id int64) {
			defer wg.Done()
			s, err := pool.NewSession(c, id)
			if err != nil {
				assert.NoError(t, err)
				return
			}
			defer s.Close()

			time.Sleep(5 * time.Second)
		}(int64(id))
	}

	wg.Wait()
}
