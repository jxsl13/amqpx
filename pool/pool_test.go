package pool_test

import (
	"sync"
	"testing"

	"github.com/jxsl13/amqpx/pool"
	"github.com/stretchr/testify/assert"
)

func TestNewConnectionPool(t *testing.T) {
	p, err := pool.NewConnectionPool("amqp://admin:password@localhost:5672", 200,
		pool.WithName("TestNewConnectionPool"),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	var wg sync.WaitGroup

	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := p.GetConnection()
			if err != nil {
				assert.NoError(t, err)
				return
			}
			p.ReturnConnection(c, false)
		}()
	}

	wg.Wait()
}
