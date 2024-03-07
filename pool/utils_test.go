package pool_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jxsl13/amqpx/internal/testutils"
	"github.com/jxsl13/amqpx/logging"
	"github.com/jxsl13/amqpx/pool"
	"github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type Consumer interface {
	Consume(queue string, option ...pool.ConsumeOptions) (<-chan amqp091.Delivery, error)
}

func ConsumeN(
	t *testing.T,
	ctx context.Context,
	wg *sync.WaitGroup,
	c Consumer,
	queueName string,
	consumerName string,
	messageGenerator func() string,
	n int,
) {
	cctx, ccancel := context.WithCancel(ctx)
	defer ccancel()
	log := logging.NewTestLogger(t)

	msgsReceived := 0
	defer func() {
		assert.Equal(t, n, msgsReceived, "expected to consume %d messages, got %d", n, msgsReceived)
	}()
outer:
	for {
		delivery, err := c.Consume(
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
}

func ConsumeAsyncN(
	t *testing.T,
	ctx context.Context,
	wg *sync.WaitGroup,
	c Consumer,
	queueName string,
	consumerName string,
	messageGenerator func() string,
	n int,
) {

	wg.Add(1)
	go func() {
		defer wg.Done()
		ConsumeN(t, ctx, wg, c, queueName, consumerName, messageGenerator, n)
	}()
}

type Producer interface {
	Publish(ctx context.Context, exchange string, routingKey string, msg pool.Publishing) (deliveryTag uint64, err error)
	IsConfirmable() bool
	AwaitConfirm(ctx context.Context, expectedTag uint64) error
}

func PublishAsyncN(
	t *testing.T,
	ctx context.Context,
	wg *sync.WaitGroup,
	p Producer,
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

				tag, err := p.Publish(
					tctx,
					exchangeName, "",
					pool.Publishing{
						Mandatory:   true,
						ContentType: "text/plain",
						Body:        []byte(publishMessageGenerator()),
					})
				if err != nil {
					assert.NoError(t, err, "expected no error when publishing message")
					return
				}
				if p.IsConfirmable() {
					err = p.AwaitConfirm(tctx, tag)
					if err != nil {
						assert.NoError(t, err, "expected no error when awaiting confirmation")
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
		assert.NoError(t, err, "expected no error when declaring exchange")
		return
	}
	defer func() {
		if err != nil {
			assert.NoError(t, s.ExchangeDelete(ctx, exchangeName), "expected no error when deleting exchange")
		}
	}()

	_, err = s.QueueDeclare(ctx, queueName)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer func() {
		if err != nil {
			_, e := s.QueueDelete(ctx, queueName)
			assert.NoError(t, e, "expected no error when deleting queue")
			// TODO: asserting the number of purged messages seems to be flaky, so we do not do that for now.
			//assert.Equalf(t, 0, deleted, "expected 0 deleted messages, got %d for queue %s", deleted, queueName)
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
		require.NoError(t, err, "expected no error when creating new connection")
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
		assert.NoError(t, err, "expected no error when creating new session")
		return nil, cleanup
	}
	return s, func() {
		assert.NoError(t, s.Close(), "expected no error when closing session")
		assert.NoError(t, c.Close(), "expected no error when closing connection")
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
			assert.Equal(t, n, i, "expected %d reconnect attempts, got %d", n, i)
		}
}
