package pool_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

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
	c Consumer,
	queueName string,
	consumerName string,
	messageGenerator func() string,
	n int,
	allowDuplicates bool,
) {
	cctx, ccancel := context.WithCancel(ctx)
	defer ccancel()
	log := logging.NewTestLogger(t)

	msgsReceived := 0
	defer func() {
		assert.Equal(t, n, msgsReceived, "expected to consume %d messages, got %d", n, msgsReceived)
	}()

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

		var previouslyReceivedMsg string

		for {
			select {
			case <-cctx.Done():
				return
			case val, ok := <-delivery:
				require.True(t, ok, "expected delivery channel to be open of consumer %s in ConsumeN", consumerName)
				if !ok {
					return
				}
				err := val.Ack(false)
				if err != nil {
					assert.NoError(t, err)
					return
				}

				var receivedMsg = string(val.Body)
				if allowDuplicates && receivedMsg == previouslyReceivedMsg {
					// TODO: it is possible that messages are duplicated, but this is not a problem
					// due to network issues. We should not fail the test in this case.
					log.Warnf("received duplicate message: %s", receivedMsg)
					continue
				}

				var expectedMsg = messageGenerator()
				assert.Equalf(
					t,
					expectedMsg,
					receivedMsg,
					"expected message %s, got %s, previously received message: %s",
					expectedMsg,
					receivedMsg,
					previouslyReceivedMsg,
				)

				log.Infof("consumed message: %s", receivedMsg)
				msgsReceived++
				if msgsReceived == n {
					logging.NewTestLogger(t).Infof("consumed %d messages, closing consumer", n)
					ccancel()
				}
				// update last received message
				previouslyReceivedMsg = receivedMsg
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
	alllowDuplicates bool,
) {

	wg.Add(1)
	go func() {
		defer wg.Done()
		ConsumeN(t, ctx, c, queueName, consumerName, messageGenerator, n, alllowDuplicates)
	}()
}

type Producer interface {
	Publish(ctx context.Context, exchange string, routingKey string, msg pool.Publishing) (deliveryTag uint64, err error)
	IsConfirmable() bool
	AwaitConfirm(ctx context.Context, expectedTag uint64) error
}

func PublishN(
	t *testing.T,
	ctx context.Context,
	p Producer,
	exchangeName string,
	publishMessageGenerator func() string,
	n int,
) {
	for i := 0; i < n; i++ {
		message := publishMessageGenerator()
		err := publish(ctx, p, exchangeName, message)
		assert.NoError(t, err)
	}
	logging.NewTestLogger(t).Infof("published %d messages, closing publisher", n)
}

func publish(ctx context.Context, p Producer, exchangeName string, message string) error {
	tag, err := p.Publish(
		ctx,
		exchangeName, "",
		pool.Publishing{
			Mandatory:   true,
			ContentType: "text/plain",
			Body:        []byte(message),
		})
	if err != nil {
		return fmt.Errorf("expected no error when publishing message: %w", err)
	}
	if p.IsConfirmable() {
		err = p.AwaitConfirm(ctx, tag)
		if err != nil {
			return fmt.Errorf("expected no error when awaiting confirmation: %w", err)
		}
	}
	return nil
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
		PublishN(t, ctx, p, exchangeName, publishMessageGenerator, n)
	}(wg, publishMessageGenerator, n)
}

type Topologer interface {
	ExchangeDeclare(ctx context.Context, name string, kind pool.ExchangeKind, option ...pool.ExchangeDeclareOptions) error
	ExchangeDelete(ctx context.Context, name string, option ...pool.ExchangeDeleteOptions) error
	QueueDeclare(ctx context.Context, name string, option ...pool.QueueDeclareOptions) (pool.Queue, error)
	QueueDelete(ctx context.Context, name string, option ...pool.QueueDeleteOptions) (purgedMsgs int, err error)
	QueueBind(ctx context.Context, queueName string, routingKey string, exchange string, option ...pool.QueueBindOptions) error
	QueueUnbind(ctx context.Context, name string, routingKey string, exchange string, arg ...amqp091.Table) error
}

func DeclareExchangeQueue(
	t *testing.T,
	ctx context.Context,
	s Topologer,
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
