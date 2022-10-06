package pool_test

import (
	"sync"
	"testing"
	"time"

	"github.com/jxsl13/amqpx/logging"
	"github.com/jxsl13/amqpx/pool"
	"github.com/stretchr/testify/assert"
)

func TestNewSessionPool(t *testing.T) {
	connections := 1
	sessions := 10
	p, err := pool.NewConnectionPool("amqp://admin:password@localhost:5672", connections,
		pool.ConnectionPoolWithName("TestNewConnectionPool"),
		pool.ConnectionPoolWithLogger(logging.NewTestLogger(t)),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}

	sp, err := pool.NewSessionPool(p, sessions, pool.SessionPoolWithAutoCloseConnectionPool(true))
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer sp.Close()

	var wg sync.WaitGroup

	for i := 0; i < sessions*2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, err := sp.GetSession()
			if err != nil {
				assert.NoError(t, err)
				return
			}
			time.Sleep(3 * time.Second)
			sp.ReturnSession(s, false)
		}()
	}

	wg.Wait()
}
