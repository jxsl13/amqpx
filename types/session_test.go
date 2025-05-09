package types_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jxsl13/amqpx/internal/amqputils"
	"github.com/jxsl13/amqpx/internal/proxyutils"
	"github.com/jxsl13/amqpx/internal/testlogger"
	"github.com/jxsl13/amqpx/internal/testutils"
	"github.com/jxsl13/amqpx/types"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
)

func TestNewSingleSessionPublishAndConsume(t *testing.T) {
	t.Parallel()
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
		numMsgs                 = 20
	)

	c, err := types.NewConnection(
		ctx,
		testutils.HealthyConnectURL,
		connName,
		types.ConnectionWithLogger(testlogger.NewTestLogger(t)),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer func() {
		assert.NoError(t, c.Close())
	}()

	s, err := types.NewSession(
		c,
		sessionName,
		types.SessionWithConfirms(true),
		types.SessionWithRetryCallback(
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

	cleanup := amqputils.DeclareExchangeQueue(t, ctx, s, exchangeName, queueName)
	defer cleanup()

	amqputils.ConsumeAsyncN(t, ctx, &wg, s, queueName, consumerName, consumeMessageGenerator, numMsgs, true)
	amqputils.PublishAsyncN(t, ctx, &wg, s, exchangeName, publishMessageGenerator, numMsgs)

	wg.Wait()
}

func TestManyNewSessionsPublishAndConsume(t *testing.T) {
	t.Parallel()
	var (
		ctx             = context.TODO()
		wg              sync.WaitGroup
		nextConnName    = testutils.ConnectionNameGenerator()
		connName        = nextConnName()
		nextSessionName = testutils.SessionNameGenerator(connName)
		sessions        = 5
		numMsgs         = 20
	)

	c, err := types.NewConnection(
		ctx,
		testutils.HealthyConnectURL,
		connName,
		types.ConnectionWithLogger(testlogger.NewTestLogger(t)),
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

		s, err := types.NewSession(
			c,
			sessionName,
			types.SessionWithConfirms(true),
		)
		if err != nil {
			assert.NoError(t, err)
			return
		}
		defer func() {
			assert.NoError(t, s.Close())
		}()

		cleanup := amqputils.DeclareExchangeQueue(t, ctx, s, exchangeName, queueName)
		defer cleanup()

		amqputils.ConsumeAsyncN(t, ctx, &wg, s, queueName, consumerName, consumeNextMessage, numMsgs, true)
		amqputils.PublishAsyncN(t, ctx, &wg, s, exchangeName, publishNextMessage, numMsgs)
	}

	wg.Wait()
}

func TestNewSessionQueueDeclarePassive(t *testing.T) {
	t.Parallel()
	var (
		ctx             = context.TODO()
		wg              sync.WaitGroup
		nextConnName    = testutils.ConnectionNameGenerator()
		connName        = nextConnName()
		nextSessionName = testutils.SessionNameGenerator(connName)
		sessionName     = nextSessionName()
		nextQueueName   = testutils.QueueNameGenerator(sessionName)
	)

	conn, err := types.NewConnection(
		ctx,
		testutils.HealthyConnectURL,
		connName,
		types.ConnectionWithLogger(testlogger.NewTestLogger(t)),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer func() {
		assert.NoError(t, conn.Close()) // can be nil or error
	}()

	session, err := types.NewSession(
		conn,
		sessionName,
		types.SessionWithConfirms(true),
		types.SessionWithRetryCallback(
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

	c, err := types.NewConnection(
		ctx,
		connectURL,
		connName,
		types.ConnectionWithLogger(testlogger.NewTestLogger(t)),
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

		disconnected, reconnected = proxyutils.Disconnect(t, proxyName, 5*time.Second)
	)
	s, err := types.NewSession(c, sessionName)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer func() {
		assert.NoError(t, s.Close())
	}()

	disconnected()
	err = s.ExchangeDeclare(ctx, exchangeName, types.ExchangeKindTopic)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	reconnected()

	err = s.ExchangeDelete(ctx, exchangeName)
	assert.NoError(t, err)
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

	c, err := types.NewConnection(
		ctx,
		connectURL,
		connName,
		types.ConnectionWithLogger(testlogger.NewTestLogger(t)),
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

		disconnected, reconnected = proxyutils.Disconnect(t, proxyName, 5*time.Second)
	)
	s, err := types.NewSession(c, sessionName)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer func() {
		assert.NoError(t, s.Close())
	}()

	err = s.ExchangeDeclare(ctx, exchangeName, types.ExchangeKindTopic)
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

	c, err := types.NewConnection(
		ctx,
		connectURL,
		connName,
		types.ConnectionWithLogger(testlogger.NewTestLogger(t)),
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

		disconnected, reconnected = proxyutils.Disconnect(t, proxyName, 5*time.Second)
	)
	s, err := types.NewSession(c, sessionName)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer func() {
		assert.NoError(t, s.Close())
	}()

	disconnected()

	_, err = s.QueueDeclare(ctx, queueName)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	reconnected()

	_, err = s.QueueDelete(ctx, queueName)
	assert.NoError(t, err, "expected no error when deleting queue")
	// TODO: asserting the number of deleted messages seems to be pretty flaky, so we do not assert it here
	// assert.Equal(t, 0, delMsgs, "expected 0 messages to be deleted")
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

	c, err := types.NewConnection(
		ctx,
		connectURL,
		connName,
		types.ConnectionWithLogger(testlogger.NewTestLogger(t)),
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

		disconnected, reconnected = proxyutils.Disconnect(t, proxyName, 5*time.Second)
	)
	s, err := types.NewSession(c, sessionName)
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
	delMsgs, err := s.QueueDelete(ctx, queueName)
	assert.NoError(t, err)
	assert.Equal(t, 0, delMsgs, "expected 0 messages to be deleted")
	reconnected()
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

	c, err := types.NewConnection(
		ctx,
		connectURL,
		connName,
		types.ConnectionWithLogger(testlogger.NewTestLogger(t)),
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

		disconnected, reconnected = proxyutils.Disconnect(t, proxyName, 5*time.Second)
	)
	s, err := types.NewSession(c, sessionName)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer func() {
		assert.NoError(t, s.Close())
	}()

	err = s.ExchangeDeclare(ctx, exchangeName, types.ExchangeKindTopic)
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

	c, err := types.NewConnection(
		ctx,
		connectURL,
		connName,
		types.ConnectionWithLogger(testlogger.NewTestLogger(t)),
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

		disconnected, reconnected = proxyutils.Disconnect(t, proxyName, 5*time.Second)
	)
	s, err := types.NewSession(c, sessionName)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer func() {
		assert.NoError(t, s.Close())
	}()

	err = s.ExchangeDeclare(ctx, exchangeName, types.ExchangeKindTopic)
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

	hs, hsclose := amqputils.NewSession(
		t,
		ctx,
		testutils.HealthyConnectURL,
		nextConnName(),
	)
	defer hsclose()

	s, sclose := amqputils.NewSession(
		t,
		ctx,
		connectURL,
		nextConnName(),
	)
	defer sclose()

	var (
		nextExchangeName = testutils.ExchangeNameGenerator(hs.Name())
		nextQueueName    = testutils.QueueNameGenerator(hs.Name())

		exchangeName     = nextExchangeName()
		queueName        = nextQueueName()
		nextConsumerName = testutils.ConsumerNameGenerator(queueName)
	)

	cleanup := amqputils.DeclareExchangeQueue(t, ctx, hs, exchangeName, queueName)
	defer cleanup()

	var (
		cctx, cancel              = context.WithCancel(ctx)
		wg                        sync.WaitGroup
		consumeMsgGen             = testutils.MessageGenerator(queueName)
		publishMsgGen             = testutils.MessageGenerator(queueName)
		numMsgs                   = 20
		disconnected, reconnected = proxyutils.Disconnect(t, proxyName, 5*time.Second)
	)
	defer cancel()

	amqputils.ConsumeAsyncN(t, cctx, &wg, hs, queueName, nextConsumerName(), consumeMsgGen, numMsgs, true)

	disconnected()
	amqputils.PublishN(t, ctx, s, exchangeName, publishMsgGen, numMsgs)
	reconnected()

	time.Sleep(5 * time.Second)

	cancel()
	wg.Wait()
}

func TestNewSessionConsumeWithDisconnect(t *testing.T) {
	t.Parallel()
	var (
		proxyName, connectURL, _ = testutils.NextConnectURL()
		ctx                      = context.TODO()
		nextConnName             = testutils.ConnectionNameGenerator()
	)

	hs, hsclose := amqputils.NewSession(
		t,
		ctx,
		testutils.HealthyConnectURL,
		nextConnName(),
	)
	defer hsclose()

	s, sclose := amqputils.NewSession(
		t,
		ctx,
		connectURL,
		nextConnName(),
	)
	defer sclose()

	var (
		nextExchangeName = testutils.ExchangeNameGenerator(hs.Name())
		nextQueueName    = testutils.QueueNameGenerator(hs.Name())

		exchangeName     = nextExchangeName()
		queueName        = nextQueueName()
		nextConsumerName = testutils.ConsumerNameGenerator(queueName)
	)

	cleanup := amqputils.DeclareExchangeQueue(t, ctx, hs, exchangeName, queueName)
	defer cleanup()

	var (
		publisherMsgGen           = testutils.MessageGenerator(queueName)
		consumerMsgGen            = testutils.MessageGenerator(queueName)
		numMsgs                   = 20
		wg                        sync.WaitGroup
		disconnected, reconnected = proxyutils.Disconnect(t, proxyName, 5*time.Second)
	)

	amqputils.PublishAsyncN(t, ctx, &wg, hs, exchangeName, publisherMsgGen, numMsgs)

	disconnected()
	amqputils.ConsumeN(t, ctx, s, queueName, nextConsumerName(), consumerMsgGen, numMsgs, false)
	reconnected()

	wg.Wait()
}

/*
// FIXME: out of memory tests are disabled until https://github.com/rabbitmq/amqp091-go/issues/253 is resolved
func TestChannelFullChainOnOutOfMemoryRabbitMQ(t *testing.T) {
	t.Parallel()

	var (
		ctx              = context.TODO()
		log              = testlogger.NewTestLogger(t)
		nextConnName     = testutils.ConnectionNameGenerator()
		nextSessionName  = testutils.SessionNameGenerator(nextConnName())
		sessionName      = nextSessionName()
		nextExchangeName = testutils.ExchangeNameGenerator(sessionName)
		nextQueueName    = testutils.QueueNameGenerator(sessionName)
		exchangeName     = nextExchangeName()
		queueName        = nextQueueName()
		bufferSize       = 1
	)

	log.Info("creating connection")
	conn, err := amqp.Dial(
		testutils.BrokenConnectURL, // out of memory rabbitmq
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer func() {
		assert.NoError(t, conn.Close(), "expected no error when closing connection")
	}()

	log.Info("registering flow control notification channel")
	blocked := make(chan amqp.Blocking, bufferSize)
	conn.NotifyBlocked(blocked)

	log.Info("creating channel")
	c, err := conn.Channel()
	if err != nil {
		assert.NoError(t, err, "expected no error when creating channel")
		return
	}
	defer func() {
		log.Info("closing channel")
		err = c.Close()
		assert.NoError(t, err, "expected no error when closing channel")
	}()

	log.Info("registering error notification channel")
	errors := make(chan *amqp091.Error, bufferSize)
	c.NotifyClose(errors)

	log.Info("registering confirms notification channel")
	confirms := make(chan amqp.Confirmation, bufferSize)
	c.NotifyPublish(confirms)
	err = c.Confirm(false)
	if err != nil {
		assert.NoError(t, err, "expected no error when enabling confirms")
		return
	}

	log.Info("registering flow control notification channel")
	flow := make(chan bool, bufferSize)
	c.NotifyFlow(flow)

	log.Info("registering returned message notification channel")
	returned := make(chan amqp091.Return, bufferSize)
	c.NotifyReturn(returned)

	log.Info("declaring exchange")
	err = c.ExchangeDeclare(exchangeName, "topic", true, false, false, false, nil)
	if err != nil {
		assert.NoError(t, err, "expected no error when declaring exchange")
		return
	}
	defer func() {
		log.Info("deleting exchange")
		err = c.ExchangeDelete(exchangeName, false, false)
		assert.NoError(t, err, "expected no error when deleting exchange")
	}()

	log.Info("declaring queue")
	_, err = c.QueueDeclare(queueName, true, false, false, false, nil)
	if err != nil {
		assert.NoError(t, err, "expected no error when declaring queue")
		return
	}
	defer func() {
		log.Info("deleting queue")
		_, err = c.QueueDelete(queueName, false, false, false)
		assert.NoError(t, err, "expected no error when deleting queue")
	}()

	log.Info("binding queue")
	err = c.QueueBind(queueName, "#", exchangeName, false, nil)
	if err != nil {
		assert.NoError(t, err, "expected no error when binding queue")
		return
	}
	defer func() {
		log.Info("unbinding queue")
		err = c.QueueUnbind(queueName, "#", exchangeName, nil)
		if err != nil {
			assert.NoError(t, err, "expected no error when unbinding queue")
			return
		}
	}()

	log.Info("publishing message")
	msg := "hello world"
	err = c.PublishWithContext(ctx, exchangeName, "", false, false, amqp.Publishing{
		ContentType: "text/plain",
		Body:        []byte(msg),
	})
	if err != nil {
		assert.NoError(t, err, "expected no error when publishing message")
		return
	}

	tctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	select {
	case f, ok := <-flow:
		if !ok {
			assert.Fail(t, "expected flow channel to be open")
			return
		}
		assert.Fail(t, "expected no flow message when publishing message", "got=%v", f)
	case confirm, ok := <-confirms:
		if !ok {
			assert.Fail(t, "expected confirms channel to be open")
			return
		}
		assert.Fail(t, "expected no confirmation when publishing message", "got=%v", confirm)
	case e, ok := <-errors:
		if !ok {
			assert.Fail(t, "expected errors channel to be open")
			return
		}
		assert.Fail(t, "expected no error when publishing message", "got=%v", e)
	case r, ok := <-returned:
		if !ok {
			assert.Fail(t, "expected returned channel to be open")
			return
		}
		assert.Fail(t, "expected no returned message when publishing message", "got=%v", r)
	case <-tctx.Done():
		assert.NoError(t, tctx.Err(), "expected no timeout when waiting for flow channel")
	case b, ok := <-blocked:
		if !ok {
			assert.Fail(t, "expected blocked channel to be open")
			return
		}
		assert.True(t, b.Active, "expected blocked notification to be active")
		assert.NotEmpty(t, b.Reason, "expected blocked notification to have a reason")
	}
}
*/

func TestChannelCloseWithDisconnect(t *testing.T) {
	t.Parallel()
	var (
		proxyName, connectURL, _  = testutils.NextConnectURL()
		disconnected, reconnected = proxyutils.Disconnect(t, proxyName, 5*time.Second)
	)

	amqpConn, err := amqp.Dial(connectURL)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	// only check that there is no deadlock
	defer amqpConn.Close()

	amqpChan, err := amqpConn.Channel()
	if err != nil {
		assert.NoError(t, err, "expected no error when creating channel")
		return
	}

	disconnected()
	defer reconnected()

	// only check that there is no deadlock
	_ = amqpChan.Close()
}

func TestNewSingleSessionCloseWithDisconnect(t *testing.T) {
	t.Parallel()
	var (
		ctx                       = context.TODO()
		nextConnName              = testutils.ConnectionNameGenerator()
		connName                  = nextConnName()
		nextSessionName           = testutils.SessionNameGenerator(connName)
		sessionName               = nextSessionName()
		proxyName, connectURL, _  = testutils.NextConnectURL()
		disconnected, reconnected = proxyutils.Disconnect(t, proxyName, 5*time.Second)
	)

	c, err := types.NewConnection(
		ctx,
		connectURL,
		connName,
		types.ConnectionWithLogger(testlogger.NewTestLogger(t)),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer func() {
		assert.NoError(t, c.Close())
	}()

	s, err := types.NewSession(
		c,
		sessionName,
		types.SessionWithConfirms(true),
		types.SessionWithRetryCallback(
			func(operation, connName, sessionName string, retry int, err error) {
				assert.NoErrorf(t, err, "operation=%s connName=%s sessionName=%s retry=%d", operation, connName, sessionName, retry)
			},
		),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}

	disconnected()
	defer reconnected()
	_ = s.Close()
}

/*
// FIXME: out of memory tests are disabled until https://github.com/rabbitmq/amqp091-go/issues/253 is resolved
func TestNewSingleSessionCloseWithOutOfMemoryRabbitMQ(t *testing.T) {
	t.Parallel()
	var (
		ctx              = context.TODO()
		log              = testlogger.NewTestLogger(t)
		nextConnName     = testutils.ConnectionNameGenerator()
		connName         = nextConnName()
		nextSessionName  = testutils.SessionNameGenerator(connName)
		sessionName      = nextSessionName()
		nextExchangeName = testutils.ExchangeNameGenerator(sessionName)
		nextQueueName    = testutils.QueueNameGenerator(sessionName)
		exchangeName     = nextExchangeName()
		queueName        = nextQueueName()
	)

	c, err := types.NewConnection(
		ctx,
		testutils.BrokenConnectURL, // out of memory rabbitmq
		connName,
		types.ConnectionWithLogger(log),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer func() {
		assert.NoError(t, c.Close())
	}()

	s, err := types.NewSession(
		c,
		sessionName,
		types.SessionWithConfirms(true),
		types.SessionWithRetryCallback(
			func(operation, connName, sessionName string, retry int, err error) {
				assert.NoErrorf(t, err, "operation=%s connName=%s sessionName=%s retry=%d", operation, connName, sessionName, retry)
			},
		),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}

	cleanup := amqputils.DeclareExchangeQueue(t, ctx, s, exchangeName, queueName)
	defer cleanup()

	log.Info(fmt.Sprintf("publishing message to exchange %s", exchangeName))
	tag, err := s.Publish(ctx, exchangeName, "",
		types.Publishing{
			ContentType: "text/plain",
			Body:        []byte("hello world"),
		},
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}

	log.Info(fmt.Sprintf("awaiting confirm for tag %d", tag))
	err = s.AwaitConfirm(ctx, tag)
	assert.Error(t, err, "expected a flow control error")
	cleanup()

	log.Info(fmt.Sprintf("closing session %s", s.Name()))
	err = s.Close()
	assert.NoError(t, err)
}
*/

func TestNewSingleSessionCloseWithHealthyRabbitMQ(t *testing.T) {
	t.Parallel()
	var (
		ctx              = context.TODO()
		log              = testlogger.NewTestLogger(t)
		nextConnName     = testutils.ConnectionNameGenerator()
		connName         = nextConnName()
		nextSessionName  = testutils.SessionNameGenerator(connName)
		sessionName      = nextSessionName()
		nextExchangeName = testutils.ExchangeNameGenerator(sessionName)
		nextQueueName    = testutils.QueueNameGenerator(sessionName)
		exchangeName     = nextExchangeName()
		queueName        = nextQueueName()
	)

	c, err := types.NewConnection(
		ctx,
		testutils.HealthyConnectURL, // healthy rabbitmq
		connName,
		types.ConnectionWithLogger(log),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer func() {
		assert.NoError(t, c.Close())
	}()

	s, err := types.NewSession(
		c,
		sessionName,
		types.SessionWithConfirms(true),
		types.SessionWithRetryCallback(
			func(operation, connName, sessionName string, retry int, err error) {
				assert.NoErrorf(t, err, "operation=%s connName=%s sessionName=%s retry=%d", operation, connName, sessionName, retry)
			},
		),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}

	cleanup := amqputils.DeclareExchangeQueue(t, ctx, s, exchangeName, queueName)
	defer cleanup()

	log.Info(fmt.Sprintf("publishing message to exchange %s", exchangeName))
	tag, err := s.Publish(ctx, exchangeName, "",
		types.Publishing{
			ContentType: "text/plain",
			Body:        []byte("hello world"),
		},
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}

	log.Info(fmt.Sprintf("awaiting confirm for tag %d", tag))
	err = s.AwaitConfirm(ctx, tag)
	log.Info(fmt.Sprintf("await confirm failed(as expected): %v", err))
	assert.NoError(t, err)

	cleanup()

	log.Info(fmt.Sprintf("closing session %s", s.Name()))
	err = s.Close()
	assert.NoError(t, err)
}
