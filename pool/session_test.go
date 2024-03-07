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

func TestNewSingleSessionPublishAndConsume(t *testing.T) {
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

	reconnectCB, deferredAssert := AssertConnectionReconnectAttempts(t, 0)
	defer deferredAssert()
	c, err := pool.NewConnection(
		ctx,
		testutils.HealthyConnectURL,
		connName,
		pool.ConnectionWithLogger(logging.NewTestLogger(t)),
		pool.ConnectionWithRecoverCallback(reconnectCB),
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

	ConsumeAsyncN(t, ctx, &wg, s, queueName, consumerName, consumeMessageGenerator, 20)
	PublishAsyncN(t, ctx, &wg, s, exchangeName, publishMessageGenerator, 20)

	wg.Wait()
}

func TestManyNewSessionsPublishAndConsume(t *testing.T) {
	t.Parallel() // can be run in parallel because the connection to the rabbitmq is never broken

	var (
		ctx             = context.TODO()
		wg              sync.WaitGroup
		nextConnName    = testutils.ConnectionNameGenerator()
		connName        = nextConnName()
		nextSessionName = testutils.SessionNameGenerator(connName)
		sessions        = 5
	)

	reconnectCB, deferredAssert := AssertConnectionReconnectAttempts(t, 0)
	defer deferredAssert()
	c, err := pool.NewConnection(
		ctx,
		testutils.HealthyConnectURL,
		connName,
		pool.ConnectionWithLogger(logging.NewTestLogger(t)),
		pool.ConnectionWithRecoverCallback(reconnectCB),
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

		ConsumeAsyncN(t, ctx, &wg, s, queueName, consumerName, consumeNextMessage, 20)
		PublishAsyncN(t, ctx, &wg, s, exchangeName, publishNextMessage, 20)
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

	reconnectCB, deferredAssert := AssertConnectionReconnectAttempts(t, 0)
	defer deferredAssert()
	conn, err := pool.NewConnection(
		ctx,
		testutils.HealthyConnectURL,
		connName,
		pool.ConnectionWithLogger(logging.NewTestLogger(t)),
		pool.ConnectionWithRecoverCallback(reconnectCB),
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
	t.Parallel()

	var (
		ctx                      = context.TODO()
		proxyName, connectURL, _ = testutils.NextConnectURL()
		nextConnName             = testutils.ConnectionNameGenerator()
		connName                 = nextConnName()
		nextSessionName          = testutils.SessionNameGenerator(connName)
	)

	reconnectCB, deferredAssert := AssertConnectionReconnectAttempts(t, 1)
	defer deferredAssert()
	c, err := pool.NewConnection(
		ctx,
		connectURL,
		connName,
		pool.ConnectionWithLogger(logging.NewTestLogger(t)),
		pool.ConnectionWithRecoverCallback(reconnectCB),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer func() {
		assert.NoError(t, c.Close())
	}()

	var (
		sessionName      = nextSessionName()
		nextExchangeName = testutils.ExchangeNameGenerator(sessionName)
		exchangeName     = nextExchangeName()

		disconnected, reconnected = Disconnect(t, proxyName, 5*time.Second)
	)
	s, err := pool.NewSession(c, sessionName)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer func() {
		assert.NoError(t, s.Close())
	}()

	disconnected()
	defer reconnected()

	err = s.ExchangeDeclare(ctx, exchangeName, pool.ExchangeKindTopic)
	if err != nil {
		assert.NoError(t, err)
		return
	}

	defer func() {
		err := s.ExchangeDelete(ctx, exchangeName)
		assert.NoError(t, err)
	}()
}

func TestNewSessionExchangeDeleteWithDisconnect(t *testing.T) {
	t.Parallel()

	var (
		ctx                      = context.TODO()
		proxyName, connectURL, _ = testutils.NextConnectURL()
		nextConnName             = testutils.ConnectionNameGenerator()
		connName                 = nextConnName()
		nextSessionName          = testutils.SessionNameGenerator(connName)
	)

	reconnectCB, deferredAssert := AssertConnectionReconnectAttempts(t, 1)
	defer deferredAssert()
	c, err := pool.NewConnection(
		ctx,
		connectURL,
		connName,
		pool.ConnectionWithLogger(logging.NewTestLogger(t)),
		pool.ConnectionWithRecoverCallback(reconnectCB),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer func() {
		assert.NoError(t, c.Close())
	}()

	var (
		sessionName      = nextSessionName()
		nextExchangeName = testutils.ExchangeNameGenerator(sessionName)
		exchangeName     = nextExchangeName()

		disconnected, reconnected = Disconnect(t, proxyName, 5*time.Second)
	)
	s, err := pool.NewSession(c, sessionName)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer func() {
		assert.NoError(t, s.Close())
	}()

	err = s.ExchangeDeclare(ctx, exchangeName, pool.ExchangeKindTopic)
	if err != nil {
		assert.NoError(t, err)
		return
	}

	disconnected()
	defer reconnected()

	err = s.ExchangeDelete(ctx, exchangeName)
	assert.NoError(t, err)
}

func TestNewSessionQueueDeclareWithDisconnect(t *testing.T) {
	t.Parallel()

	var (
		ctx                      = context.TODO()
		proxyName, connectURL, _ = testutils.NextConnectURL()
		nextConnName             = testutils.ConnectionNameGenerator()
		connName                 = nextConnName()
		nextSessionName          = testutils.SessionNameGenerator(connName)
	)

	reconnectCB, deferredAssert := AssertConnectionReconnectAttempts(t, 1)
	defer deferredAssert()
	c, err := pool.NewConnection(
		ctx,
		connectURL,
		connName,
		pool.ConnectionWithLogger(logging.NewTestLogger(t)),
		pool.ConnectionWithRecoverCallback(reconnectCB),
	)
	if err != nil {
		assert.NoError(t, err, "expected no error when creating new connection")
		return
	}
	defer func() {
		assert.NoError(t, c.Close(), "expected no error when closing connection")
	}()

	var (
		sessionName   = nextSessionName()
		nextQueueName = testutils.QueueNameGenerator(sessionName)
		queueName     = nextQueueName()

		disconnected, reconnected = Disconnect(t, proxyName, 5*time.Second)
	)
	s, err := pool.NewSession(c, sessionName)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer func() {
		assert.NoError(t, s.Close())
	}()

	disconnected()
	defer reconnected()

	_, err = s.QueueDeclare(ctx, queueName)
	if err != nil {
		assert.NoError(t, err)
		return
	}

	defer func() {
		_, err := s.QueueDelete(ctx, queueName)
		assert.NoError(t, err, "expected no error when deleting queue")
		// TODO: asserting the number of deleted messages seems to be pretty flaky, so we do not assert it here
		// assert.Equal(t, 0, delMsgs, "expected 0 messages to be deleted")
	}()
}

func TestNewSessionQueueDeleteWithDisconnect(t *testing.T) {
	t.Parallel()

	var (
		ctx                      = context.TODO()
		proxyName, connectURL, _ = testutils.NextConnectURL()
		nextConnName             = testutils.ConnectionNameGenerator()
		connName                 = nextConnName()
		nextSessionName          = testutils.SessionNameGenerator(connName)
	)

	reconnectCB, deferredAssert := AssertConnectionReconnectAttempts(t, 1)
	defer deferredAssert()
	c, err := pool.NewConnection(
		ctx,
		connectURL,
		connName,
		pool.ConnectionWithLogger(logging.NewTestLogger(t)),
		pool.ConnectionWithRecoverCallback(reconnectCB),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer func() {
		assert.NoError(t, c.Close())
	}()

	var (
		sessionName   = nextSessionName()
		nextQueueName = testutils.QueueNameGenerator(sessionName)
		queueName     = nextQueueName()

		disconnected, reconnected = Disconnect(t, proxyName, 5*time.Second)
	)
	s, err := pool.NewSession(c, sessionName)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer func() {
		assert.NoError(t, s.Close())
	}()

	_, err = s.QueueDeclare(ctx, queueName)
	if err != nil {
		assert.NoError(t, err)
		return
	}

	disconnected()
	defer reconnected()

	delMsgs, err := s.QueueDelete(ctx, queueName)
	assert.NoError(t, err)
	assert.Equal(t, 0, delMsgs, "expected 0 messages to be deleted")
}

func TestNewSessionQueueBindWithDisconnect(t *testing.T) {
	t.Parallel()

	var (
		ctx                      = context.TODO()
		proxyName, connectURL, _ = testutils.NextConnectURL()
		nextConnName             = testutils.ConnectionNameGenerator()
		connName                 = nextConnName()
		nextSessionName          = testutils.SessionNameGenerator(connName)
	)

	reconnectCB, deferredAssert := AssertConnectionReconnectAttempts(t, 1)
	defer deferredAssert()
	c, err := pool.NewConnection(
		ctx,
		connectURL,
		connName,
		pool.ConnectionWithLogger(logging.NewTestLogger(t)),
		pool.ConnectionWithRecoverCallback(reconnectCB),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer func() {
		assert.NoError(t, c.Close())
	}()

	var (
		sessionName      = nextSessionName()
		nextExchangeName = testutils.ExchangeNameGenerator(sessionName)
		exchangeName     = nextExchangeName()
		nextQueueName    = testutils.QueueNameGenerator(sessionName)
		queueName        = nextQueueName()

		disconnected, reconnected = Disconnect(t, proxyName, 5*time.Second)
	)
	s, err := pool.NewSession(c, sessionName)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer func() {
		assert.NoError(t, s.Close())
	}()

	err = s.ExchangeDeclare(ctx, exchangeName, pool.ExchangeKindTopic)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer func() {
		err = s.ExchangeDelete(ctx, exchangeName)
		assert.NoError(t, err)
	}()

	_, err = s.QueueDeclare(ctx, queueName)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer func() {
		delMsgs, err := s.QueueDelete(ctx, queueName)
		assert.NoError(t, err)
		assert.Equal(t, 0, delMsgs, "expected 0 messages to be deleted")
	}()

	disconnected()
	defer reconnected()

	err = s.QueueBind(ctx, queueName, "#", exchangeName)
	if err != nil {
		assert.NoError(t, err)
		return
	}
}

func TestNewSessionQueueUnbindWithDisconnect(t *testing.T) {
	t.Parallel()

	var (
		ctx                      = context.TODO()
		proxyName, connectURL, _ = testutils.NextConnectURL()
		nextConnName             = testutils.ConnectionNameGenerator()
		connName                 = nextConnName()
		nextSessionName          = testutils.SessionNameGenerator(connName)
	)

	reconnectCB, deferredAssert := AssertConnectionReconnectAttempts(t, 1)
	defer deferredAssert()
	c, err := pool.NewConnection(
		ctx,
		connectURL,
		connName,
		pool.ConnectionWithLogger(logging.NewTestLogger(t)),
		pool.ConnectionWithRecoverCallback(reconnectCB),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer func() {
		assert.NoError(t, c.Close())
	}()

	var (
		sessionName      = nextSessionName()
		nextExchangeName = testutils.ExchangeNameGenerator(sessionName)
		exchangeName     = nextExchangeName()
		nextQueueName    = testutils.QueueNameGenerator(sessionName)
		queueName        = nextQueueName()

		disconnected, reconnected = Disconnect(t, proxyName, 5*time.Second)
	)
	s, err := pool.NewSession(c, sessionName)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer func() {
		assert.NoError(t, s.Close())
	}()

	err = s.ExchangeDeclare(ctx, exchangeName, pool.ExchangeKindTopic)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer func() {
		err = s.ExchangeDelete(ctx, exchangeName)
		assert.NoError(t, err)
	}()

	_, err = s.QueueDeclare(ctx, queueName)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer func() {
		delMsgs, err := s.QueueDelete(ctx, queueName)
		assert.NoError(t, err)
		assert.Equal(t, 0, delMsgs, "expected 0 messages to be deleted")
	}()

	err = s.QueueBind(ctx, queueName, "#", exchangeName)
	if err != nil {
		assert.NoError(t, err)
		return
	}

	disconnected()
	defer reconnected()

	err = s.QueueUnbind(ctx, queueName, "#", exchangeName, nil)
	assert.NoError(t, err)
}

func TestNewSessionPublishWithDisconnect(t *testing.T) {
	t.Parallel()

	var (
		proxyName, connectURL, _ = testutils.NextConnectURL()
		ctx                      = context.TODO()
		nextConnName             = testutils.ConnectionNameGenerator()
	)

	healthyConnCB, hcbAssert := AssertConnectionReconnectAttempts(t, 0)
	defer hcbAssert()
	hs, hsclose := NewSession(
		t,
		ctx,
		testutils.HealthyConnectURL,
		nextConnName(),
		pool.ConnectionWithRecoverCallback(healthyConnCB),
	)
	defer hsclose()

	brokenReconnCB, scbAssert := AssertConnectionReconnectAttempts(t, 1)
	defer scbAssert()
	s, sclose := NewSession(
		t,
		ctx,
		connectURL,
		nextConnName(),
		pool.ConnectionWithRecoverCallback(brokenReconnCB),
	)
	defer sclose()

	var (
		nextExchangeName = testutils.ExchangeNameGenerator(hs.Name())
		nextQueueName    = testutils.QueueNameGenerator(hs.Name())

		exchangeName     = nextExchangeName()
		queueName        = nextQueueName()
		nextConsumerName = testutils.ConsumerNameGenerator(queueName)
	)

	cleanup := DeclareExchangeQueue(t, ctx, hs, exchangeName, queueName)
	defer cleanup()

	var (
		msgGen = func() string { return "test message content" }
		wg     sync.WaitGroup
	)

	ConsumeAsyncN(t, ctx, &wg, hs, queueName, nextConsumerName(), msgGen, 1)

	disconnected, reconnected := Disconnect(t, proxyName, 5*time.Second)
	disconnected()
	PublishAsyncN(t, ctx, &wg, s, exchangeName, msgGen, 1)
	reconnected()

	wg.Wait()
}

/*
func TestNewSessionWithDisconnect(t *testing.T) {

	var (
		ctx                      = context.TODO()
		proxyName, connectURL, _ = testutils.NextConnectURL()
		nextConnName             = testutils.ConnectionNameGenerator()
		connName                 = nextConnName()
		nextSessionName          = testutils.SessionNameGenerator(connName)
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

	start, started, stopped := DisconnectWithStartStartedStopped(t, proxyName, time.Second)
	start2, started2, stopped2 := DisconnectWithStartStartedStopped(t, proxyName, time.Second)
	start3, started3, stopped3 := DisconnectWithStartStartedStopped(t, proxyName, time.Second)
	start4, started4, stopped4 := DisconnectWithStartStartedStopped(t, proxyName, time.Second)
	start5, started5, stopped5 := DisconnectWithStartStartedStopped(t, proxyName, time.Second)
	start6, started6, stopped6 := DisconnectWithStartStartedStopped(t, proxyName, time.Second)

	// deferred
	start7, started7, stopped7 := DisconnectWithStartStartedStopped(t, proxyName, time.Second)
	start8, started8, stopped8 := DisconnectWithStartStartedStopped(t, proxyName, time.Second)
	start9, started9, stopped9 := DisconnectWithStartStartedStopped(t, proxyName, time.Second)
	start10, started10, stopped10 := DisconnectWithStartStartedStopped(t, proxyName, time.Second)

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

*/
