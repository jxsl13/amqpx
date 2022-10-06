package pool_test

import (
	"sync"
	"testing"
	"time"

	"github.com/jxsl13/amqpx/logging"
	"github.com/jxsl13/amqpx/pool"
	"github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// QuorumArgs is the argument you need to pass in order to create a quorum queue.
var QuorumArgs = amqp091.Table{
	"x-queue-type": "quorum",
}

func TestNew(t *testing.T) {
	connections := 2
	sessions := 10

	p, err := pool.New("amqp://admin:password@localhost:5672", connections, sessions,
		pool.WithName("TestNew"),
		pool.WithLogger(logging.NewTestLogger(t)),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer p.Close()

	var wg sync.WaitGroup

	for i := 0; i < sessions*2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			session, err := p.GetSession()
			if err != nil {
				assert.NoError(t, err)
				return
			}
			time.Sleep(1 * time.Second)

			p.ReturnSession(session, false)
		}()
	}

	wg.Wait()

}
