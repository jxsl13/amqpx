package pool_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jxsl13/amqpx"
	"github.com/jxsl13/amqpx/logging"
	"github.com/jxsl13/amqpx/pool"
	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
)

var (
	connectURL       = amqpx.NewURL("localhost", 5672, "admin", "password")
	brokenConnectURL = amqpx.NewURL("localhost", 5673, "admin", "password")
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(
		m,
		goleak.IgnoreTopFunction("internal/poll.runtime_pollWait"),
		goleak.IgnoreTopFunction("github.com/rabbitmq/amqp091-go.(*Connection).heartbeater"),
		goleak.IgnoreTopFunction("net/http.(*persistConn).writeLoop"),
	)
}

func TestNew(t *testing.T) {
	ctx := context.TODO()
	connections := 2
	sessions := 10

	p, err := pool.New(
		ctx,
		connectURL,
		connections,
		sessions,
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

			session, err := p.GetSession(ctx)
			if err != nil {
				assert.NoError(t, err)
				return
			}
			time.Sleep(1 * time.Second)

			p.ReturnSession(ctx, session, false)
		}()
	}

	wg.Wait()

}
