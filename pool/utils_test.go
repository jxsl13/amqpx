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
	"github.com/stretchr/testify/require"
)

func ConsumeAsyncN(
	t *testing.T,
	ctx context.Context,
	wg *sync.WaitGroup,
	s *pool.Session,
	queueName string,
	consumerName string,
	messageGenerator func() string,
	n int,
) {

	wg.Add(1)
	go func(wg *sync.WaitGroup, messageGenerator func() string, n int) {
		defer wg.Done()
		cctx, ccancel := context.WithCancel(ctx)
		defer ccancel()
		log := logging.NewTestLogger(t)

		msgsReceived := 0
		defer func() {
			assert.Equal(t, n, msgsReceived)
		}()
	outer:
		for {
			delivery, err := s.Consume(
				queueName,
				pool.ConsumeOptions{
					ConsumerTag: consumerName,
					Exclusive:   true,
				},
			)
			if err != nil {
				assert.NoError(t, err)
				return
			}

			for {
				select {
				case <-cctx.Done():
					return
				case val, ok := <-delivery:
					if !ok {
						continue outer
					}
					err := val.Ack(false)
					if err != nil {
						assert.NoError(t, err)
						return
					}

					receivedMsg := string(val.Body)
					assert.Equal(t, messageGenerator(), receivedMsg)
					log.Infof("consumed message: %s", receivedMsg)
					msgsReceived++
					if msgsReceived == n {
						logging.NewTestLogger(t).Infof("consumed %d messages, closing consumer", n)
						ccancel()
					}
				}
			}
		}
	}(wg, messageGenerator, n)
}

func PublishAsyncN(
	t *testing.T,
	ctx context.Context,
	wg *sync.WaitGroup,
	s *pool.Session,
	exchangeName string,
	publishMessageGenerator func() string,
	n int,
) {
	wg.Add(1)
	go func(wg *sync.WaitGroup, publishMessageGenerator func() string, n int) {
		defer wg.Done()

		for i := 0; i < n; i++ {
			func() {
				tctx, cancel := context.WithTimeout(ctx, 60*time.Second)
				defer cancel()

				tag, err := s.Publish(
					tctx,
					exchangeName, "",
					pool.Publishing{
						Mandatory:   true,
						ContentType: "text/plain",
						Body:        []byte(publishMessageGenerator()),
					})
				if err != nil {
					assert.NoError(t, err)
					return
				}
				if s.IsConfirmable() {
					err = s.AwaitConfirm(tctx, tag)
					if err != nil {
						assert.NoError(t, err)
						return
					}
				}
			}()
		}
		logging.NewTestLogger(t).Infof("published %d messages, closing publisher", n)
	}(wg, publishMessageGenerator, n)
}

func DeclareExchangeQueue(
	t *testing.T,
	ctx context.Context,
	s *pool.Session,
	exchangeName string,
	queueName string,
) (cleanup func()) {
	cleanup = func() {}
	var err error

	err = s.ExchangeDeclare(ctx, exchangeName, pool.ExchangeKindTopic)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer func() {
		if err != nil {
			assert.NoError(t, s.ExchangeDelete(ctx, exchangeName))
		}
	}()

	_, err = s.QueueDeclare(ctx, queueName)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer func() {
		if err != nil {
			deleted, e := s.QueueDelete(ctx, queueName)
			assert.NoError(t, e)
			assert.Equalf(t, 0, deleted, "expected 0 deleted messages, got %d for queue %s", deleted, queueName)
		}
	}()

	err = s.QueueBind(ctx, queueName, "#", exchangeName)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer func() {
		if err != nil {
			assert.NoError(t, s.QueueUnbind(ctx, queueName, "#", exchangeName, nil))
		}
	}()

	return func() {
		assert.NoError(t, s.QueueUnbind(ctx, queueName, "#", exchangeName, nil))

		_, e := s.QueueDelete(ctx, queueName)
		assert.NoError(t, e)
		assert.NoError(t, s.ExchangeDelete(ctx, exchangeName))
	}
}

func NewSession(t *testing.T, ctx context.Context, connectURL, connectionName string, options ...pool.ConnectionOption) (_ *pool.Session, cleanup func()) {
	cleanup = func() {}
	log := logging.NewTestLogger(t)
	c, err := pool.NewConnection(
		ctx,
		connectURL,
		connectionName,
		append([]pool.ConnectionOption{
			pool.ConnectionWithLogger(log),
		}, options...)...)
	if err != nil {
		require.NoError(t, err)
		return nil, cleanup
	}
	nextSessionName := testutils.SessionNameGenerator(connectionName)
	s, err := pool.NewSession(
		c,
		nextSessionName(),
		pool.SessionWithConfirms(true),
		pool.SessionWithLogger(log),
		pool.SessionWithRetryCallback(func(operation, connName, sessionName string, retry int, err error) {
			log.Infof("retrying %s on connection %s, session %s, attempt %d, error: %s", operation, connName, sessionName, retry, err)
		}),
	)
	if err != nil {
		require.NoError(t, err)
		return nil, cleanup
	}
	return s, func() {
		assert.NoError(t, s.Close())
		assert.NoError(t, c.Close())
	}
}

func AssertConnectionReconnectAttempts(t *testing.T, n int) (callback pool.ConnectionRecoverCallback, deferredAssert func()) {
	var (
		i   int
		mu  sync.Mutex
		log = logging.NewTestLogger(t)
	)
	return func(name string, retry int, err error) {
			if retry == 0 {
				log.Infof("connection %s retry %d, error: %v", name, retry, err)
				mu.Lock()
				i++
				mu.Unlock()
			}
		},
		func() {
			mu.Lock()
			defer mu.Unlock()
			assert.Equal(t, n, i)
		}
}
