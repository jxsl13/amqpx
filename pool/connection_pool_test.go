package pool_test

import (
	"sync"
	"testing"
	"time"

	"github.com/jxsl13/amqpx/logging"
	"github.com/jxsl13/amqpx/pool"
	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestNewConnectionPool(t *testing.T) {
	connections := 5
	p, err := pool.NewConnectionPool("amqp://admin:password@localhost:5672", connections,
		pool.ConnectionPoolWithName("TestNewConnectionPool"),
		pool.ConnectionPoolWithLogger(logging.NewTestLogger(t)),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer p.Close()
	var wg sync.WaitGroup

	for i := 0; i < connections; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := p.GetConnection()
			if err != nil {
				assert.NoError(t, err)
				return
			}
			time.Sleep(5 * time.Second)
			p.ReturnConnection(c, false)
		}()
	}

	wg.Wait()
}
