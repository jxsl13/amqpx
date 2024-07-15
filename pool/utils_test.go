package pool_test

import (
	"context"
	"fmt"
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

	var previouslyReceivedMsg string

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
	Publish(ctx context.Context, exchange string, routingKey string, msg pool.Publishing) (confirm *pool.Confirmation, err error)
	PublishBatch(ctx context.Context, msgs []pool.BatchPublishing) (confirm *pool.BatchConfirmation, err error)
	IsConfirmable() bool
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

func PublishBatchN(
	t *testing.T,
	ctx context.Context,
	p Producer,
	exchangeNames []string,
	publishMessageGenerators []func() string,
	n int,
) {
	msgs := make([]pool.BatchPublishing, n)
	for i := 0; i < n; i++ {
		message := publishMessageGenerators[i]()
		msgs[i] = pool.BatchPublishing{
			Exchange: exchangeNames[i],
			Publishing: pool.Publishing{
				ContentType: "text/plain",
				Body:        []byte(message),
			},
		}
	}

	err := publishBatch(ctx, p, msgs)
	assert.NoError(t, err)
	logging.NewTestLogger(t).Infof("published %d messages, closing publisher", n)
}

func publish(ctx context.Context, p Producer, exchangeName string, message string) error {
	confirm, err := p.Publish(
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
		err = confirm.Wait(ctx)
		if err != nil {
			return fmt.Errorf("expected no error when awaiting confirmation: %w", err)
		}
	}
	return nil
}

func publishBatch(ctx context.Context, p Producer, msgs []pool.BatchPublishing) error {
	// loop to retry when confirmation failes
	for {
		confirm, err := p.PublishBatch(ctx, msgs)
		if err != nil {
			return fmt.Errorf("expected no error when publishing batch message: %w", err)
		}
		if p.IsConfirmable() {
			err = confirm.Wait(ctx)
			if err != nil {
				continue
			}
		}
		break
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

func PublishBatchAsyncN(
	t *testing.T,
	ctx context.Context,
	wg *sync.WaitGroup,
	p Producer,
	exchangeNames []string,
	publishMessageGenerators []func() string,
	n int,
) {
	wg.Add(1)
	go func(wg *sync.WaitGroup, publishMessageGenerators []func() string, n int) {
		defer wg.Done()
		PublishBatchN(t, ctx, p, exchangeNames, publishMessageGenerators, n)
	}(wg, publishMessageGenerators, n)
}

type Topologer interface {
	ExchangeDeclare(ctx context.Context, name string, kind pool.ExchangeKind, option ...pool.ExchangeDeclareOptions) error
	ExchangeDelete(ctx context.Context, name string, option ...pool.ExchangeDeleteOptions) error
	QueueDeclare(ctx context.Context, name string, option ...pool.QueueDeclareOptions) (pool.Queue, error)
	QueueDelete(ctx context.Context, name string, option ...pool.QueueDeleteOptions) (purgedMsgs int, err error)
	QueueBind(ctx context.Context, queueName string, routingKey string, exchange string, option ...pool.QueueBindOptions) error
	QueueUnbind(ctx context.Context, name string, routingKey string, exchange string, arg ...amqp091.Table) error
}

// DeclareExchangeQueue declares an exchange and a queue and binds them together.
// It returns a cleanup function that can be used to delete the exchange and queue.
// The cleanup function is idempotent and can be called multiple times, but it will only delete the exchange and queue once.
func DeclareExchangeQueue(
	t *testing.T,
	ctx context.Context,
	s Topologer,
	exchangeName string,
	queueName string,
) (cleanup func()) {
	cleanup = func() {}
	var err error

	log := logging.NewTestLogger(t)

	log.Infof("declaring exchange %s", exchangeName)
	err = s.ExchangeDeclare(ctx, exchangeName, pool.ExchangeKindTopic)
	if err != nil {
		assert.NoError(t, err, "expected no error when declaring exchange")
		return
	}
	defer func() {
		if err != nil {
			log.Infof("deleting exchange %s", exchangeName)
			assert.NoError(t, s.ExchangeDelete(ctx, exchangeName), "expected no error when deleting exchange")
		}
	}()

	log.Infof("declaring queue %s", queueName)
	_, err = s.QueueDeclare(ctx, queueName)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer func() {
		if err != nil {
			log.Infof("deleting queue %s", queueName)
			_, e := s.QueueDelete(ctx, queueName)
			assert.NoError(t, e, "expected no error when deleting queue")
			// TODO: asserting the number of purged messages seems to be flaky, so we do not do that for now.
			//assert.Equalf(t, 0, deleted, "expected 0 deleted messages, got %d for queue %s", deleted, queueName)
		}
	}()

	log.Infof("binding queue %s to exchange %s", queueName, exchangeName)
	err = s.QueueBind(ctx, queueName, "#", exchangeName)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer func() {
		if err != nil {
			log.Infof("unbinding queue %s from exchange %s", queueName, exchangeName)
			assert.NoError(t, s.QueueUnbind(ctx, queueName, "#", exchangeName, nil))
		}
	}()

	once := sync.Once{}
	return func() {
		once.Do(func() {
			log.Infof("unbinding queue %s from exchange %s", queueName, exchangeName)
			assert.NoError(t, s.QueueUnbind(ctx, queueName, "#", exchangeName, nil))

			log.Infof("deleting queue %s", queueName)
			_, e := s.QueueDelete(ctx, queueName)
			assert.NoError(t, e)

			log.Infof("deleting exchange %s", exchangeName)
			assert.NoError(t, s.ExchangeDelete(ctx, exchangeName))
		})
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
		log.Infof("closing session %s", s.Name())
		assert.NoError(t, s.Close(), "expected no error when closing session")
		log.Infof("closing connection %s", c.Name())
		assert.NoError(t, c.Close(), "expected no error when closing connection")
	}
}

func PublisherPublishN(t *testing.T, ctx context.Context, p *pool.Pool, exchangeName string, publishMessageGenerator func() string, n int) {
	pub := pool.NewPublisher(p)
	defer pub.Close()

	for i := 0; i < n; i++ {
		message := publishMessageGenerator()
		err := pub.Publish(ctx, exchangeName, "", pool.Publishing{
			ContentType: "text/plain",
			Body:        []byte(message),
		})
		assert.NoError(t, err)
	}
}

func PublisherPublishAsyncN(
	t *testing.T,
	ctx context.Context,
	wg *sync.WaitGroup,
	p *pool.Pool,
	exchangeName string,
	publishMessageGenerator func() string,
	n int,
) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		PublisherPublishN(t, ctx, p, exchangeName, publishMessageGenerator, n)
	}()
}

func SubscriberConsumeN(
	t *testing.T,
	ctx context.Context,
	p *pool.Pool,
	queueName string,
	consumerName string,
	messageGenerator func() string,
	n int,
	allowDuplicates bool,
) {
	var log = logging.NewTestLogger(t)

	processingTime := 30 * time.Millisecond
	cctx, ccancel := context.WithTimeout(ctx, 30*time.Second+time.Duration((2+1)*n)*processingTime)
	defer ccancel()
	sub := pool.NewSubscriber(
		p,
		pool.SubscriberWithContext(cctx),
		pool.SubscriberWithLogger(log),
	)
	defer sub.Close()

	msgsReceived := 0
	defer func() {
		assert.Equal(t, n, msgsReceived, "expected to consume %d messages, got %d", n, msgsReceived)
	}()

	var previouslyReceivedMsg string
	sub.RegisterHandlerFunc(queueName, func(ctx context.Context, d pool.Delivery) error {
		var receivedMsg = string(d.Body)

		select {
		case <-time.After(testutils.Jitter(0, processingTime)):
		case <-ctx.Done():
		}

		if allowDuplicates && receivedMsg == previouslyReceivedMsg {
			// TODO: it is possible that messages are duplicated, but this is not a problem
			// due to network issues. We should not fail the test in this case.
			log.Warnf("received duplicate message: %s", receivedMsg)
			return nil
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
		return nil
	}, pool.ConsumeOptions{
		ConsumerTag: consumerName,
	})

	err := sub.Start(cctx)
	if err != nil {
		assert.NoError(t, err)
		ccancel()
	}
	sub.Wait()
}

func SubscriberConsumeAsyncN(
	t *testing.T,
	ctx context.Context,
	wg *sync.WaitGroup,
	p *pool.Pool,
	queueName string,
	consumerName string,
	messageGenerator func() string,
	n int,
	allowDuplicates bool,
) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		SubscriberConsumeN(t, ctx, p, queueName, consumerName, messageGenerator, n, allowDuplicates)
	}()
}

func SubscriberBatchConsumeN(
	t *testing.T,
	ctx context.Context,
	p *pool.Pool,
	queueName string,
	consumerName string,
	messageGenerator func() string,
	n int,
	batchSize int,
	maxBytes int,
	allowDuplicates bool,
) {
	var log = logging.NewTestLogger(t)
	processingTime := 30 * time.Millisecond
	cctx, ccancel := context.WithTimeout(ctx, 30*time.Second+time.Duration((2+1)*n)*processingTime)
	defer ccancel()
	sub := pool.NewSubscriber(
		p,
		pool.SubscriberWithContext(cctx),
		pool.SubscriberWithLogger(log),
	)
	defer sub.Close()

	msgsReceived := 0
	defer func() {
		assert.Equal(t, n, msgsReceived, "expected to consume %d messages, got %d", n, msgsReceived)
	}()

	var previouslyReceivedMsg string
	sub.RegisterBatchHandlerFunc(queueName, func(ctx context.Context, ds []pool.Delivery) error {
		for _, d := range ds {
			var receivedMsg = string(d.Body)

			select {
			case <-time.After(testutils.Jitter(0, processingTime)):
			case <-ctx.Done():
			}

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
		return nil
	}, pool.WithBatchConsumeOptions(pool.ConsumeOptions{
		ConsumerTag: consumerName,
	}), pool.WithMaxBatchSize(batchSize),
		pool.WithMaxBatchBytes(maxBytes),
	)

	err := sub.Start(cctx)
	if err != nil {
		assert.NoError(t, err)
		ccancel()
	}
	sub.Wait()
}

func SubscriberBatchConsumeAsyncN(
	t *testing.T,
	ctx context.Context,
	wg *sync.WaitGroup,
	p *pool.Pool,
	queueName string,
	consumerName string,
	messageGenerator func() string,
	n int,
	batchSize int,
	maxBytes int,
	allowDuplicates bool,
) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		SubscriberBatchConsumeN(
			t,
			ctx,
			p,
			queueName,
			consumerName,
			messageGenerator,
			n,
			batchSize,
			maxBytes,
			allowDuplicates,
		)
	}()
}

func NewPool(t *testing.T, ctx context.Context, connectURL, poolName string, numConns, numSessions int) *pool.Pool {
	p, err := pool.New(
		ctx,
		connectURL,
		numConns,
		numSessions,
		pool.WithLogger(logging.NewTestLogger(t)),
		pool.WithConfirms(true),
		pool.WithName(poolName),
	)
	if err != nil {
		assert.NoError(t, err)
		return nil
	}
	return p
}
