package pool_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jxsl13/amqpx/internal/testutils"
	"github.com/jxsl13/amqpx/logging"
	"github.com/jxsl13/amqpx/pool"
	"github.com/stretchr/testify/assert"
)

func TestNewSingleSession(t *testing.T) {
	t.Parallel() // can be run in parallel because the connection to the rabbitmq is never broken

	var (
		ctx                     = context.TODO()
		wg                      sync.WaitGroup
		nextConnName            = testutils.ConnectionNameGenerator()
		connName                = nextConnName()
		nextSessionName         = testutils.SessionNameGenerator(connName)
		sessionName             = nextSessionName()
		nextQueueName           = testutils.QueueNameGenerator(sessionName)
		queueName               = nextQueueName()
		nextExchangeName        = testutils.ExchangeNameGenerator(sessionName)
		exchangeName            = nextExchangeName()
		nextConsumerName        = testutils.ConsumerNameGenerator(queueName)
		consumerName            = nextConsumerName()
		consumeMessageGenerator = testutils.MessageGenerator(queueName)
		publishMessageGenerator = testutils.MessageGenerator(queueName)
	)

	c, err := pool.NewConnection(
		ctx,
		connectURL,
		connName,
		pool.ConnectionWithLogger(logging.NewTestLogger(t)),
		pool.ConnectionWithRecoverCallback(func(name string, retry int, err error) {
			assert.NoErrorf(t, err, "name=%s retry=%d", name, retry)
		}),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer func() {
		assert.NoError(t, c.Close())
	}()

	s, err := pool.NewSession(
		c,
		sessionName,
		pool.SessionWithConfirms(true),
		pool.SessionWithRetryCallback(
			func(operation, connName, sessionName string, retry int, err error) {
				assert.NoErrorf(t, err, "operation=%s connName=%s sessionName=%s retry=%d", operation, connName, sessionName, retry)
			},
		),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer func() {
		assert.NoError(t, s.Close())
	}()

	cleanup := DeclareExchangeQueue(t, ctx, s, exchangeName, queueName)
	defer cleanup()

	ConsumeAsyncN(t, ctx, &wg, s, queueName, consumerName, consumeMessageGenerator, 1)
	PublishAsyncN(t, ctx, &wg, s, exchangeName, publishMessageGenerator, 1)

	wg.Wait()
}

func TestManyNewSessions(t *testing.T) {
	t.Parallel() // can be run in parallel because the connection to the rabbitmq is never broken

	var (
		ctx             = context.TODO()
		wg              sync.WaitGroup
		nextConnName    = testutils.ConnectionNameGenerator()
		connName        = nextConnName()
		nextSessionName = testutils.SessionNameGenerator(connName)
		sessions        = 5
	)

	c, err := pool.NewConnection(
		ctx,
		connectURL,
		connName,
		pool.ConnectionWithLogger(logging.NewTestLogger(t)),
		pool.ConnectionWithRecoverCallback(func(name string, retry int, err error) {
			assert.NoErrorf(t, err, "unextected connection recovery: name=%s retry=%d", name, retry)
		}),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer func() {
		assert.NoError(t, c.Close())
	}()

	for id := 0; id < sessions; id++ {
		var (
			sessionName      = nextSessionName()
			nextQueueName    = testutils.QueueNameGenerator(sessionName)
			queueName        = nextQueueName()
			nextExchangeName = testutils.ExchangeNameGenerator(sessionName)
			exchangeName     = nextExchangeName()
			nextConsumerName = testutils.ConsumerNameGenerator(queueName)
			consumerName     = nextConsumerName()
			// generate equal consume & publish messages for comparison
			consumeNextMessage = testutils.MessageGenerator(queueName)
			publishNextMessage = testutils.MessageGenerator(queueName)
		)

		s, err := pool.NewSession(
			c,
			sessionName,
			pool.SessionWithConfirms(true),
		)
		if err != nil {
			assert.NoError(t, err)
			return
		}
		defer func() {
			assert.NoError(t, s.Close())
		}()

		cleanup := DeclareExchangeQueue(t, ctx, s, exchangeName, queueName)
		defer cleanup()

		ConsumeAsyncN(t, ctx, &wg, s, queueName, consumerName, consumeNextMessage, 1)
		PublishAsyncN(t, ctx, &wg, s, exchangeName, publishNextMessage, 1)
	}

	wg.Wait()
}

func TestNewSessionQueueDeclarePassive(t *testing.T) {
	t.Parallel() // can be run in parallel because the connection to the rabbitmq is never broken

	var (
		ctx             = context.TODO()
		wg              sync.WaitGroup
		nextConnName    = testutils.ConnectionNameGenerator()
		connName        = nextConnName()
		nextSessionName = testutils.SessionNameGenerator(connName)
		sessionName     = nextSessionName()
		nextQueueName   = testutils.QueueNameGenerator(sessionName)
	)

	conn, err := pool.NewConnection(
		ctx,
		connectURL,
		connName,
		pool.ConnectionWithLogger(logging.NewTestLogger(t)),
		pool.ConnectionWithRecoverCallback(func(name string, retry int, err error) {
			assert.NoErrorf(t, err, "unextected connection recovery: name=%s retry=%d", name, retry)
		}),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer func() {
		assert.NoError(t, conn.Close()) // can be nil or error
	}()

	session, err := pool.NewSession(
		conn,
		sessionName,
		pool.SessionWithConfirms(true),
		pool.SessionWithRetryCallback(
			func(operation, connName, sessionName string, retry int, err error) {
				assert.NoErrorf(t, err, "unexpected session recovery: operation=%s connName=%s sessionName=%s retry=%d", operation, connName, sessionName, retry)
			},
		),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer func() {
		assert.NoError(t, session.Close())
	}()

	for i := 0; i < 100; i++ {
		func() {
			qname := nextQueueName()
			q, err := session.QueueDeclare(ctx, qname)
			if err != nil {
				assert.NoError(t, err)
				return
			}
			assert.Equalf(t, 0, q.Consumers, "expected 0 consumers when declaring a queue: %s", qname)

			// executed upon return
			defer func() {
				_, err := session.QueueDelete(ctx, qname)
				assert.NoErrorf(t, err, "failed to delete queue: %s", qname)
			}()

			q, err = session.QueueDeclarePassive(ctx, qname)
			if err != nil {
				assert.NoErrorf(t, err, "QueueDeclarePassive failed for queue: %s", qname)
				return
			}

			assert.Equalf(t, 0, q.Consumers, "queue should not have any consumers: %s", qname)
		}()
	}

	wg.Wait()
}

func TestNewSessionExchangeDeclareWithDisconnect(t *testing.T) {
	var (
		ctx             = context.TODO()
		nextConnName    = testutils.ConnectionNameGenerator()
		connName        = nextConnName()
		nextSessionName = testutils.SessionNameGenerator(connName)
	)

	var (
		reconnectCounter   int64 = 0
		expectedReconnects int64 = 1
	)

	defer func() {
		assert.Equal(t, expectedReconnects, reconnectCounter, "number of reconnection attempts")
	}()
	c, err := pool.NewConnection(
		ctx,
		connectURL,
		connName,
		pool.ConnectionWithLogger(logging.NewTestLogger(t)),
		pool.ConnectionWithRecoverCallback(func(name string, retry int, err error) {
			if retry == 0 {
				atomic.AddInt64(&reconnectCounter, 1)
			}
		}),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer func() {
		c.Close() // can be nil or error
	}()

	var (
		sessionName      = nextSessionName()
		nextExchangeName = testutils.ExchangeNameGenerator(sessionName)
		exchangeName     = nextExchangeName()

		start, started, stopped = DisconnectWithStartStartedStopped(t, time.Second)
	)
	s, err := pool.NewSession(c, sessionName)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer s.Close() // can be nil or error

	start() // await connection loss start
	started()

	err = s.ExchangeDeclare(ctx, exchangeName, pool.ExchangeKindTopic)
	if err != nil {
		assert.NoError(t, err)
		return
	}

	stopped()

	defer func() {
		err := s.ExchangeDelete(ctx, exchangeName)
		assert.NoError(t, err)
	}()
}

func TestNewSessionExchangeDeleteWithDisconnect(t *testing.T) {
	var (
		ctx             = context.TODO()
		nextConnName    = testutils.ConnectionNameGenerator()
		connName        = nextConnName()
		nextSessionName = testutils.SessionNameGenerator(connName)
	)

	var (
		reconnectCounter   int64 = 0
		expectedReconnects int64 = 1
	)

	defer func() {
		assert.Equal(t, expectedReconnects, reconnectCounter, "number of reconnection attempts")
	}()
	c, err := pool.NewConnection(
		ctx,
		connectURL,
		connName,
		pool.ConnectionWithLogger(logging.NewTestLogger(t)),
		pool.ConnectionWithRecoverCallback(func(name string, retry int, err error) {
			if retry == 0 {
				atomic.AddInt64(&reconnectCounter, 1)
			}
		}),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer func() {
		c.Close() // can be nil or error
	}()

	var (
		sessionName      = nextSessionName()
		nextExchangeName = testutils.ExchangeNameGenerator(sessionName)
		exchangeName     = nextExchangeName()

		start, started, stopped = DisconnectWithStartStartedStopped(t, time.Second)
	)
	s, err := pool.NewSession(c, sessionName)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer s.Close() // can be nil or error

	err = s.ExchangeDeclare(ctx, exchangeName, pool.ExchangeKindTopic)
	if err != nil {
		assert.NoError(t, err)
		return
	}

	start() // await connection loss start
	started()

	err = s.ExchangeDelete(ctx, exchangeName)
	assert.NoError(t, err)
	stopped()
}

func TestNewSessionQueueDeclareWithDisconnect(t *testing.T) {
	var (
		ctx             = context.TODO()
		nextConnName    = testutils.ConnectionNameGenerator()
		connName        = nextConnName()
		nextSessionName = testutils.SessionNameGenerator(connName)
	)

	var (
		reconnectCounter   int64 = 0
		expectedReconnects int64 = 1
	)

	defer func() {
		assert.Equal(t, expectedReconnects, reconnectCounter, "number of reconnection attempts")
	}()
	c, err := pool.NewConnection(
		ctx,
		connectURL,
		connName,
		pool.ConnectionWithLogger(logging.NewTestLogger(t)),
		pool.ConnectionWithRecoverCallback(func(name string, retry int, err error) {
			if retry == 0 {
				atomic.AddInt64(&reconnectCounter, 1)
			}
		}),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer func() {
		c.Close() // can be nil or error
	}()

	var (
		sessionName   = nextSessionName()
		nextQueueName = testutils.QueueNameGenerator(sessionName)
		queueName     = nextQueueName()

		start, started, stopped = DisconnectWithStartStartedStopped(t, time.Second)
	)
	s, err := pool.NewSession(c, sessionName)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer s.Close() // can be nil or error

	start() // await connection loss start
	started()

	_, err = s.QueueDeclare(ctx, queueName)
	if err != nil {
		assert.NoError(t, err)
		return
	}

	stopped()

	defer func() {
		delMsgs, err := s.QueueDelete(ctx, queueName)
		assert.NoError(t, err)
		assert.Equal(t, 0, delMsgs, "expected 0 messages to be deleted")
	}()
}

func TestNewSessionQueueDeleteWithDisconnect(t *testing.T) {
	var (
		ctx             = context.TODO()
		nextConnName    = testutils.ConnectionNameGenerator()
		connName        = nextConnName()
		nextSessionName = testutils.SessionNameGenerator(connName)
	)

	var (
		reconnectCounter   int64 = 0
		expectedReconnects int64 = 1
	)

	defer func() {
		assert.Equal(t, expectedReconnects, reconnectCounter, "number of reconnection attempts")
	}()
	c, err := pool.NewConnection(
		ctx,
		connectURL,
		connName,
		pool.ConnectionWithLogger(logging.NewTestLogger(t)),
		pool.ConnectionWithRecoverCallback(func(name string, retry int, err error) {
			if retry == 0 {
				atomic.AddInt64(&reconnectCounter, 1)
			}
		}),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer func() {
		c.Close() // can be nil or error
	}()

	var (
		sessionName   = nextSessionName()
		nextQueueName = testutils.QueueNameGenerator(sessionName)
		queueName     = nextQueueName()

		start, started, stopped = DisconnectWithStartStartedStopped(t, time.Second)
	)
	s, err := pool.NewSession(c, sessionName)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer s.Close() // can be nil or error

	_, err = s.QueueDeclare(ctx, queueName)
	if err != nil {
		assert.NoError(t, err)
		return
	}

	start() // await connection loss start
	started()

	delMsgs, err := s.QueueDelete(ctx, queueName)
	assert.NoError(t, err)
	assert.Equal(t, 0, delMsgs, "expected 0 messages to be deleted")
	stopped()
}

func TestNewSessionWithDisconnect(t *testing.T) {
	var (
		ctx             = context.TODO()
		nextConnName    = testutils.ConnectionNameGenerator()
		connName        = nextConnName()
		nextSessionName = testutils.SessionNameGenerator(connName)
	)

	var reconnectCounter int64 = 0
	defer func() {
		assert.Equal(t, 10, reconnectCounter-1)
	}()
	c, err := pool.NewConnection(
		ctx,
		connectURL,
		connName,
		pool.ConnectionWithLogger(logging.NewTestLogger(t)),
		pool.ConnectionWithRecoverCallback(func(name string, retry int, err error) {
			if retry == 0 {
				atomic.AddInt64(&reconnectCounter, 1)
			}
		}),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer func() {
		c.Close() // can be nil or error
	}()

	var wg sync.WaitGroup

	sessions := 1
	wg.Add(sessions)

	start, started, stopped := DisconnectWithStartStartedStopped(t, time.Second)
	start2, started2, stopped2 := DisconnectWithStartStartedStopped(t, time.Second)
	start3, started3, stopped3 := DisconnectWithStartStartedStopped(t, time.Second)
	start4, started4, stopped4 := DisconnectWithStartStartedStopped(t, time.Second)
	start5, started5, stopped5 := DisconnectWithStartStartedStopped(t, time.Second)
	start6, started6, stopped6 := DisconnectWithStartStartedStopped(t, time.Second)

	// deferred
	start7, started7, stopped7 := DisconnectWithStartStartedStopped(t, time.Second)
	start8, started8, stopped8 := DisconnectWithStartStartedStopped(t, time.Second)
	start9, started9, stopped9 := DisconnectWithStartStartedStopped(t, time.Second)
	start10, started10, stopped10 := DisconnectWithStartStartedStopped(t, time.Second)

	for id := 0; id < sessions; id++ {
		go func() {
			defer wg.Done()

			var (
				sessionName      = nextSessionName()
				nextQueueName    = testutils.QueueNameGenerator(sessionName)
				queueName        = nextQueueName()
				nextExchangeName = testutils.ExchangeNameGenerator(sessionName)
				exchangeName     = nextExchangeName()
				nextConsumerName = testutils.ConsumerNameGenerator(queueName)
				consumerName     = nextConsumerName()
				nextMessage      = testutils.MessageGenerator(queueName)
			)
			s, err := pool.NewSession(c, sessionName, pool.SessionWithConfirms(true))
			if err != nil {
				assert.NoError(t, err)
				return
			}
			defer func() {
				// INFO: does not lead to a recovery
				start10()
				started10()

				s.Close() // can be nil or error
				stopped10()
			}()

			start() // await connection loss start
			started()

			err = s.ExchangeDeclare(ctx, exchangeName, pool.ExchangeKindTopic)
			if err != nil {
				assert.NoError(t, err)
				return
			}

			stopped()

			defer func() {
				start9()
				started9()

				err := s.ExchangeDelete(ctx, exchangeName)
				assert.NoError(t, err)

				stopped9()
			}()

			start2()
			started2()

			_, err = s.QueueDeclare(ctx, queueName)
			if err != nil {
				assert.NoError(t, err)
				return
			}
			stopped2()

			defer func() {
				start8()
				started8()
				stopped8()

				_, err := s.QueueDelete(ctx, queueName)
				assert.NoError(t, err)
			}()

			start3()
			started3()

			err = s.QueueBind(ctx, queueName, "#", exchangeName)
			if err != nil {
				assert.NoError(t, err)
				return
			}
			stopped3()

			defer func() {
				start7()
				started7()

				err := s.QueueUnbind(ctx, queueName, "#", exchangeName, nil)
				assert.NoError(t, err)

				stopped7()
			}()

			start4()
			started4()

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
			stopped4()

			message := nextMessage()

			wg.Add(1)
			go func(msg string) {
				defer wg.Done()

				msgsReceived := 0
				for val := range delivery {
					receivedMsg := string(val.Body)
					assert.Equal(t, message, receivedMsg)
					msgsReceived++
				}
				// consumption fails because the connection will be closed
				assert.Equal(t, 0, msgsReceived)
				// this routine must be closed upon session closure
			}(message)

			time.Sleep(2 * time.Second)

			start5()
			started5()
			var once sync.Once

			for {
				tag, err := s.Publish(ctx, exchangeName, "", pool.Publishing{
					Mandatory:   true,
					ContentType: "application/json",
					Body:        []byte(message),
				})
				if err != nil {
					assert.NoError(t, err)
					return
				}

				stopped5()

				once.Do(func() {
					start6()
					started6()
					stopped6()
				})

				err = s.AwaitConfirm(ctx, tag)
				if err != nil {
					// retry because the first attempt at confirmation failed
					continue
				}
				break
			}

		}()
	}

	wg.Wait()
}
