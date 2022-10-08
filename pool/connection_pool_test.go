package pool_test

import (
	"sync"
	"testing"
	"time"

	"github.com/jxsl13/amqpx/logging"
	"github.com/jxsl13/amqpx/pool"
	"github.com/stretchr/testify/assert"
)

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

func TestNewConnectionPoolDisconnect(t *testing.T) {
	connections := 100 // do not go below 100
	p, err := pool.NewConnectionPool("amqp://admin:password@localhost:5672", connections,
		pool.ConnectionPoolWithName("TestNewConnectionPoolDisconnect"),
		pool.ConnectionPoolWithLogger(logging.NewTestLogger(t)),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer p.Close()
	var wg sync.WaitGroup

	awaitStarted, awaitStopped := DisconnectWithStartStop(t, 0, time.Duration(connections)*10*time.Millisecond, 2*time.Second)
	defer awaitStopped()

	for i := 0; i < connections; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			awaitStarted() // wait for connection loss

			// no connection, this should retry until there is a connection
			c, err := p.GetConnection()
			if err != nil {
				assert.NoError(t, err)
				return
			}

			time.Sleep(1 * time.Second)
			p.ReturnConnection(c, false)
		}(i)
	}

	wg.Wait()
}
