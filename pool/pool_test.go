package pool_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jxsl13/amqpx/internal/testutils"
	"github.com/jxsl13/amqpx/logging"
	"github.com/jxsl13/amqpx/pool"
	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	testutils.VerifyLeak(m)
}

func TestNewPool(t *testing.T) {

	var (
		ctx         = context.TODO()
		poolName    = testutils.FuncName()
		connections = 2
		sessions    = 10
	)

	p, err := pool.New(
		ctx,
		testutils.HealthyConnectURL,
		connections,
		sessions,
		pool.WithName(poolName),
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
			time.Sleep(testutils.Jitter(1*time.Second, 3*time.Second))

			// recovering should not be neccessary
			assert.NoError(t, session.Recover(ctx))

			p.ReturnSession(session, nil)
		}()
	}

	wg.Wait()
}
