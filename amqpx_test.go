package amqpx_test

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jxsl13/amqpx"
	"github.com/jxsl13/amqpx/internal/testlogger"
	"github.com/jxsl13/amqpx/internal/testutils"
	"github.com/jxsl13/amqpx/pool"
	"github.com/jxsl13/amqpx/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// WARNING: Do not assert consumer counts, as those values are too flaky and break tests all over the place
func TestMain(m *testing.M) {
	testutils.VerifyLeak(m)
}

func TestExchangeDeclarePassive(t *testing.T) {
	t.Parallel()
	var (
		amqp             = amqpx.New()
		ctx              = context.TODO()
		log              = testlogger.NewTestLogger(t)
		funcName         = testutils.FuncName()
		nextExchangeName = testutils.ExchangeNameGenerator(funcName)
		exchangeName     = nextExchangeName()
	)
	defer func() {
		assert.NoError(t, amqp.Close())
	}()

	var err error
	amqp.RegisterTopologyCreator(func(ctx context.Context, t *pool.Topologer) error {
		return createExchange(ctx, exchangeName, t, log)
	})

	amqp.RegisterTopologyDeleter(func(ctx context.Context, t *pool.Topologer) error {
		return deleteExchange(ctx, exchangeName, t, log)
	})

	err = amqp.Start(
		ctx,
		testutils.HealthyConnectURL,
		amqpx.WithName(funcName),
		amqpx.WithLogger(testlogger.NewTestLogger(t)),
		amqpx.WithPublisherConnections(1),
		amqpx.WithPublisherSessions(2),
		amqpx.WithCloseTimeout(time.Minute),
	)
	assert.NoError(t, err)
}

func TestQueueDeclarePassive(t *testing.T) {
	t.Parallel()
	var (
		amqp          = amqpx.New()
		ctx           = context.TODO()
		log           = testlogger.NewTestLogger(t)
		funcName      = testutils.FuncName()
		nextQueueName = testutils.QueueNameGenerator(funcName)
		queueName     = nextQueueName()
	)
	defer func() {
		assert.NoError(t, amqp.Close())
	}()

	var err error
	amqp.RegisterTopologyCreator(func(ctx context.Context, t *pool.Topologer) error {
		return createQueue(ctx, queueName, t, log)
	})

	amqp.RegisterTopologyDeleter(func(ctx context.Context, t *pool.Topologer) error {
		return deleteQueue(ctx, queueName, t, log)
	})

	err = amqp.Start(
		ctx,
		testutils.HealthyConnectURL,
		amqpx.WithName(funcName),
		amqpx.WithLogger(testlogger.NewTestLogger(t)),
		amqpx.WithPublisherConnections(1),
		amqpx.WithPublisherSessions(2),
		amqpx.WithCloseTimeout(time.Minute),
	)
	assert.NoError(t, err)
}

func TestAMQPXPub(t *testing.T) {
	t.Parallel()
	var (
		amqp     = amqpx.New()
		ctx      = context.TODO()
		log      = testlogger.NewTestLogger(t)
		funcName = testutils.FuncName()

		nextExchangeQueue = testutils.NewExchangeQueueGenerator(funcName)
		eq1               = nextExchangeQueue()
	)
	defer func() {
		assert.NoError(t, amqp.Close())
	}()

	amqp.RegisterTopologyCreator(createTopology(log, eq1))
	amqp.RegisterTopologyDeleter(deleteTopology(log, eq1))

	err := amqp.Start(
		ctx,
		testutils.HealthyConnectURL,
		amqpx.WithName(funcName),
		amqpx.WithLogger(slog.New(slog.DiscardHandler)),
		amqpx.WithPublisherConnections(1),
		amqpx.WithPublisherSessions(2),
		amqpx.WithCloseTimeout(time.Minute),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer func() {
		// will be canceled when the event has reache dthe third handler
		err = amqp.Close()
		assert.NoError(t, err)
	}()

	// publish event to first queue
	err = amqp.Publish(ctx, eq1.Exchange, eq1.RoutingKey, types.Publishing{
		ContentType: "text/plain",
		Body:        []byte(eq1.NextPubMsg()),
	})
	if err != nil {
		assert.NoError(t, err)
		return
	}

	var (
		msg types.Delivery
		ok  bool
	)

	for i := 0; i < 20; i++ {
		msg, ok, err = amqp.Get(ctx, eq1.Queue, false)
		if err != nil {
			assert.NoError(t, err)
			return
		}
		if !ok {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		break
	}

	if !ok {
		assert.True(t, ok)
		return
	}

	assert.Equal(t, eq1.NextSubMsg(), string(msg.Body))
}

func TestAMQPXSubAndPub(t *testing.T) {
	t.Parallel()
	var (
		amqp              = amqpx.New()
		log               = testlogger.NewTestLogger(t)
		cctx, cancel      = context.WithCancel(context.TODO())
		funcName          = testutils.FuncName()
		nextExchangeQueue = testutils.NewExchangeQueueGenerator(funcName)
		eq1               = nextExchangeQueue()
	)
	defer cancel()
	defer func() {
		log.Info("closing amqp")
		assert.NoError(t, amqp.Close())
	}()

	amqp.RegisterTopologyCreator(createTopology(log, eq1))
	amqp.RegisterTopologyDeleter(deleteTopology(log, eq1))

	// we only expect a single message to arrive or
	// a duplicate message due to network issues.
	expectedMsg := eq1.NextSubMsg()
	amqp.RegisterHandler(eq1.Queue, func(ctx context.Context, d types.Delivery) error {
		msg := string(d.Body)
		log.Info(fmt.Sprintf("subscriber of %s received message: %s", eq1.Queue, msg))
		assert.Equal(t, expectedMsg, msg)
		cancel()
		return nil
	})

	err := amqp.Start(
		cctx,
		testutils.HealthyConnectURL,
		amqpx.WithName(funcName),
		amqpx.WithLogger(slog.New(slog.DiscardHandler)),
		amqpx.WithPublisherConnections(1),
		amqpx.WithPublisherSessions(5),
		amqpx.WithCloseTimeout(time.Minute),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}

	// publish event to first queue
	// due to bad network it is possible that the message is received multiple times.
	// that is why we pass context.TODO() to the publish method in order to avoid
	// aborting a secondary retry which would return an error here.
	tctx, tcancel := context.WithTimeout(context.TODO(), 10*time.Second)
	defer tcancel()
	err = amqp.Publish(tctx, eq1.Exchange, eq1.RoutingKey, types.Publishing{
		ContentType: "text/plain",
		Body:        []byte(eq1.NextPubMsg()),
	})
	if err != nil {
		assert.NoError(t, err)
		return
	}

	// will be canceled when the event has reache dthe third handler
	<-cctx.Done()
	log.Info("context canceled, closing test.")
}

func TestAMQPXSubAndPubMulti(t *testing.T) {
	t.Parallel()
	var (
		amqp              = amqpx.New()
		log               = testlogger.NewTestLogger(t)
		cctx, cancel      = context.WithCancel(context.TODO())
		funcName          = testutils.FuncName()
		nextExchangeQueue = testutils.NewExchangeQueueGenerator(funcName)

		eq1 = nextExchangeQueue()
		eq2 = nextExchangeQueue()
		eq3 = nextExchangeQueue()
	)
	defer cancel()
	defer func() {
		log.Info("closing amqp")
		assert.NoError(t, amqp.Close())
	}()

	amqp.RegisterTopologyCreator(createTopology(log, eq1, eq2, eq3))
	amqp.RegisterTopologyDeleter(deleteTopology(log, eq1, eq2, eq3))

	// publish -> queue-01 -> subscriber-01 -> queue-02 -> subscriber-02 -> queue-03 -> subscriber-03 -> cancel context
	amqp.RegisterHandler(eq1.Queue, func(ctx context.Context, msg types.Delivery) error {
		log.Info(fmt.Sprintf("handler of %s", eq1.Queue))

		assert.Equal(t, eq1.NextSubMsg(), string(msg.Body))

		err := amqp.Publish(ctx, eq2.Exchange, eq2.RoutingKey, types.Publishing{
			ContentType: msg.ContentType,
			Body:        msg.Body,
		})

		if err != nil {
			log.Error(fmt.Sprintf("%s: %v", eq1.Queue, err))
			assert.NoError(t, err)
		}

		return nil
	},
		types.ConsumeOptions{
			ConsumerTag: eq1.ConsumerTag,
		},
	)

	amqp.RegisterHandler(eq2.Queue, func(ctx context.Context, msg types.Delivery) error {
		log.Info(fmt.Sprintf("handler of %s", eq2.Queue))

		err := amqp.Publish(ctx, eq3.Exchange, eq3.RoutingKey, types.Publishing{
			ContentType: msg.ContentType,
			Body:        msg.Body,
		})

		if err != nil {
			log.Error(fmt.Sprintf("%s: %v", eq2.Queue, err))
		}

		return nil
	}, types.ConsumeOptions{ConsumerTag: eq2.ConsumerTag})

	amqp.RegisterHandler(eq3.Queue, func(ctx context.Context, msg types.Delivery) error {
		log.Info(fmt.Sprintf("handler of %s: canceling context!", eq3.Queue))
		cancel()
		return nil
	}, types.ConsumeOptions{ConsumerTag: eq3.ConsumerTag})

	err := amqp.Start(
		cctx,
		testutils.HealthyConnectURL,
		amqpx.WithName(funcName),
		amqpx.WithLogger(log),
		amqpx.WithPublisherConnections(1),
		amqpx.WithPublisherSessions(5),
		amqpx.WithCloseTimeout(time.Minute),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}

	// publish event to first queue

	err = amqp.Publish(cctx, eq1.Exchange, eq1.RoutingKey, types.Publishing{
		ContentType: "text/plain",
		Body:        []byte(eq1.NextPubMsg()),
	})
	if err != nil {
		assert.NoError(t, err)
		return
	}

	// will be canceled when the event has reache dthe third handler
	<-cctx.Done()
	log.Info("context canceled, closing test.")
}

func TestAMQPXSubHandler(t *testing.T) {
	t.Parallel()
	var (
		amqp              = amqpx.New()
		log               = testlogger.NewTestLogger(t)
		cctx, cancel      = context.WithCancel(context.TODO())
		funcName          = testutils.FuncName()
		nextExchangeQueue = testutils.NewExchangeQueueGenerator(funcName)
		eq1               = nextExchangeQueue()
	)
	defer func() {
		log.Info("canceling context")
		cancel()

		log.Info("closing amqp")
		assert.NoError(t, amqp.Close())
	}()

	amqp.RegisterTopologyCreator(createTopology(log, eq1))
	amqp.RegisterTopologyDeleter(deleteTopology(log, eq1))

	// we want to only receive one message or one duplicate message
	expectedMsg := eq1.NextSubMsg()
	amqp.RegisterHandler(eq1.Queue, func(ctx context.Context, d types.Delivery) error {
		msg := string(d.Body)
		log.Info(fmt.Sprintf("subscriber of %s: received message: %s", eq1.Queue, msg))
		assert.Equal(t, expectedMsg, msg)
		log.Info("canceling context from within handler")
		cancel()
		return nil
	})

	err := amqp.Start(
		cctx,
		testutils.HealthyConnectURL,
		amqpx.WithName(funcName),
		amqpx.WithLogger(slog.New(slog.DiscardHandler)),
		amqpx.WithPublisherConnections(1),
		amqpx.WithPublisherSessions(5),
		amqpx.WithCloseTimeout(time.Minute),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}

	// publish event to first queue
	// due to bad network it is possible that the message is received multiple times.
	// that is why we pass context.TODO() to the publish method in order to avoid
	// aborting a secondary retry which would return an error here.
	tctx, tcancel := context.WithTimeout(context.TODO(), 10*time.Second)
	defer tcancel()
	err = amqp.Publish(tctx, eq1.Exchange, eq1.RoutingKey, types.Publishing{
		ContentType: "text/plain",
		Body:        []byte(eq1.NextPubMsg()),
	})
	if err != nil {
		assert.NoError(t, err)
		return
	}

	// will be canceled when the event has reache dthe third handler
	<-cctx.Done()
	log.Info("context canceled, closing test.")
}

func TestCreateDeleteTopology(t *testing.T) {
	t.Parallel()
	var (
		amqp              = amqpx.New()
		log               = testlogger.NewTestLogger(t)
		cctx, cancel      = context.WithCancel(context.TODO())
		funcName          = testutils.FuncName()
		nextExchangeQueue = testutils.NewExchangeQueueGenerator(funcName)
		eq1               = nextExchangeQueue()
	)
	defer cancel()
	defer func() {
		log.Info("closing amqp")
		assert.NoError(t, amqp.Close())
	}()

	amqp.RegisterTopologyCreator(createTopology(log, eq1))
	amqp.RegisterTopologyDeleter(deleteTopology(log, eq1))

	err := amqp.Start(
		cctx,
		testutils.HealthyConnectURL,
		amqpx.WithName(funcName),
		amqpx.WithLogger(log),
		amqpx.WithPublisherConnections(1),
		amqpx.WithPublisherSessions(2),
		amqpx.WithCloseTimeout(time.Minute),
	)
	assert.NoError(t, err)
}

func TestPauseResumeHandlerNoProcessing(t *testing.T) {
	t.Parallel()
	var (
		amqp          = amqpx.New()
		log           = testlogger.NewTestLogger(t)
		cctx, cancel  = context.WithCancel(context.TODO())
		funcName      = testutils.FuncName()
		nextQueueName = testutils.QueueNameGenerator(funcName)
		queueName     = nextQueueName()
	)
	defer cancel()
	defer func() {
		log.Info("closing amqp")
		assert.NoError(t, amqp.Close())
	}()

	amqp.RegisterTopologyCreator(func(ctx context.Context, t *pool.Topologer) error {
		_, err := t.QueueDeclare(ctx, queueName)
		if err != nil {
			return err
		}
		return nil
	})

	amqp.RegisterTopologyDeleter(func(ctx context.Context, t *pool.Topologer) error {
		_, err := t.QueueDelete(ctx, queueName)
		if err != nil {
			return err
		}
		return nil
	})

	handler := amqp.RegisterHandler(queueName, func(ctx context.Context, d types.Delivery) error {
		log.Info("received message")
		return nil
	})

	err := amqp.Start(
		cctx,
		testutils.HealthyConnectURL,
		amqpx.WithName(funcName),
		amqpx.WithLogger(slog.New(slog.DiscardHandler)),
		amqpx.WithCloseTimeout(time.Minute),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}

	for i := 0; i < 5; i++ {
		t.Logf("iteration %d", i)
		assertActive(t, handler, true)

		err = handler.Pause(cctx)
		if err != nil {
			assert.NoError(t, err)
			return
		}

		assertActive(t, handler, false)

		err = handler.Resume(cctx)
		if err != nil {
			assert.NoError(t, err)
			return
		}

		assertActive(t, handler, true)
	}
}

func TestHandlerPauseAndResumeSubscriber(t *testing.T) {
	t.Parallel()
	var (
		amqp              = amqpx.New()
		log               = testlogger.NewTestLogger(t)
		cctx, cancel      = context.WithCancel(context.TODO())
		funcName          = testutils.FuncName()
		nextExchangeQueue = testutils.NewExchangeQueueGenerator(funcName)
		eq1               = nextExchangeQueue()
	)
	defer cancel()
	defer func() {
		log.Info("closing amqp")
		assert.NoError(t, amqp.Close())
	}()

	options := []amqpx.Option{
		amqpx.WithLogger(slog.New(slog.DiscardHandler)),
		amqpx.WithPublisherConnections(1),
		amqpx.WithPublisherSessions(1),
		amqpx.WithCloseTimeout(time.Minute),
	}

	amqpPub := amqpx.New()
	amqpPub.RegisterTopologyCreator(createTopology(log, eq1))
	amqp.RegisterTopologyDeleter(deleteTopology(log, eq1))
	defer func() {
		assert.NoError(t, amqpPub.Close())
	}()

	err := amqpPub.Start(
		cctx,
		testutils.HealthyConnectURL,
		append(options, amqpx.WithName(funcName+"-pub"))...,
	)
	require.NoError(t, err)

	var (
		publish                = 10
		cnt                    = 0
		processingFinshed      = make(chan struct{})
		initialBatchSize       = 2
		subscriberFlushTimeout = 500 * time.Millisecond
		finalBatchSize         = 1
	)
	// step 2 - process messages, pause, wait, resume, process rest, cancel context
	handler := amqp.RegisterBatchHandler(eq1.Queue, func(hctx context.Context, msgs []types.Delivery) (err error) {
		select {
		case <-hctx.Done():
			return fmt.Errorf("handler context canceled before processing: %w", hctx.Err())
		default:
			// nothing
		}

		for _, msg := range msgs {
			assert.Equal(t, eq1.NextSubMsg(), string(msg.Body))
			cnt++
		}

		if cnt == publish {
			close(processingFinshed)
		}

		return nil
	},
		pool.WithMaxBatchSize(initialBatchSize),
		pool.WithBatchFlushTimeout(subscriberFlushTimeout),
	)

	err = amqp.Start(cctx, testutils.HealthyConnectURL,
		amqpx.WithName(funcName+"-sub"),
		amqpx.WithLogger(log),
		amqpx.WithCloseTimeout(time.Minute),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}

	// publish half of the messages
	for i := 0; i < publish/2; i++ {
		err := amqpPub.Publish(cctx, eq1.Exchange, eq1.RoutingKey, types.Publishing{
			ContentType: "text/plain",
			Body:        []byte(eq1.NextPubMsg()),
		})
		if err != nil {
			assert.NoError(t, err)
			return
		}
	}

	time.Sleep(2 * subscriberFlushTimeout)

	// pause and reduce batch size and resume
	reconnectTimeout := 2 * time.Minute

	pauseCtx, cancel := context.WithTimeout(cctx, reconnectTimeout)
	err = handler.Pause(pauseCtx)
	cancel()
	assert.NoError(t, err)

	handler.SetMaxBatchSize(finalBatchSize)

	resumeCtx, cancel := context.WithTimeout(cctx, reconnectTimeout)
	err = handler.Resume(resumeCtx)
	cancel()
	assert.NoError(t, err)

	// publish rest of messages
	for i := 0; i < publish/2; i++ {
		err := amqpPub.Publish(cctx, eq1.Exchange, eq1.RoutingKey, types.Publishing{
			ContentType: "text/plain",
			Body:        []byte(eq1.NextPubMsg()),
		})
		if err != nil {
			assert.NoError(t, err)
			return
		}
	}

	// await for subscriber to consume all messages before finishing test
	publishFinishTimeout := time.Duration(publish/2) * 500 * time.Millisecond // max one second per message
	select {
	case <-time.After(publishFinishTimeout):
		t.Errorf("timeout after %s", publishFinishTimeout)
		return
	case <-processingFinshed:
		log.Info("processing finished successfully")
	}
}

func TestHandlerPauseAndResumeInFlightNackSubscriber(t *testing.T) {
	t.Parallel()
	var (
		log               = testlogger.NewTestLogger(t)
		cctx, cancel      = context.WithCancel(context.TODO())
		funcName          = testutils.FuncName()
		nextExchangeQueue = testutils.NewExchangeQueueGenerator(funcName)
		eq1               = nextExchangeQueue()
	)
	defer cancel()

	var (
		publish                = 10
		initialBatchSize       = publish * 2     // higher than number of published messages in order to enforce messages to be in flight
		subscriberFlushTimeout = 3 * time.Second // also use a high timeout in order to enforce messages to be in flight
		cnt                    = 0
		processingFinshed      = make(chan struct{})
		finalBatchSize         = 1
		redeliveryLimit        = types.DefaultQueueDeliveryLimit - 1
		redeliveryChan         = make(chan struct{}, redeliveryLimit)
	)

	options := []amqpx.Option{
		amqpx.WithLogger(slog.New(slog.DiscardHandler)),
		amqpx.WithPublisherConnections(1),
		amqpx.WithPublisherSessions(1),
		amqpx.WithCloseTimeout(time.Minute),
	}

	// publish messages
	amqpPub := amqpx.New()
	amqpPub.RegisterTopologyCreator(createTopology(log, eq1))
	amqpPub.RegisterTopologyDeleter(deleteTopology(log, eq1))

	err := amqpPub.Start(
		cctx,
		testutils.HealthyConnectURL,
		append(options, amqpx.WithName(funcName+"-pub"))...,
	)
	if !assert.NoError(t, err) {
		return
	}
	defer func() {
		assert.NoError(t, amqpPub.Close())
	}()

	for i := 0; i < publish; i++ {
		err := amqpPub.Publish(cctx, eq1.Exchange, eq1.RoutingKey, types.Publishing{
			ContentType: "text/plain",
			Body:        []byte(eq1.NextPubMsg()),
		})
		if err != nil {
			assert.NoError(t, err)
			return
		}
	}

	// INFO: The issue here seems to be that the first message exceeds the DeliveryLimit and is added to the end of the queue.
	// which is why the order is not preserved.
	amqp := amqpx.New()

	proceedChan := make(chan struct{})
	once := sync.Once{}
	handler := amqp.RegisterBatchHandler(eq1.Queue, func(hctx context.Context, msgs []types.Delivery) (err error) {

		select {
		case <-hctx.Done():
			return hctx.Err() // additional nack & requeue
		case redeliveryChan <- struct{}{}:

			// nack & requeue the message for a limited amout of times
			return fmt.Errorf("we don't want the message to be processed, yet") // nack & requeue for every token that could be put in the channel
		default:
			once.Do(func() {
				close(proceedChan)
			})
		}

		// at this point the order is not supposed to be.
		// The bigger the batch, the more data we loose
		for _, msg := range msgs {
			assert.Equal(t, eq1.NextSubMsg(), string(msg.Body))
			cnt++
		}

		if cnt == publish {
			close(processingFinshed)
		}

		return nil
	},
		pool.WithMaxBatchSize(initialBatchSize),
		pool.WithBatchFlushTimeout(subscriberFlushTimeout),
	)

	err = amqp.Start(cctx, testutils.HealthyConnectURL,
		amqpx.WithName(funcName+"-sub"),
		amqpx.WithLogger(log),
		amqpx.WithPublisherConnections(1),
		amqpx.WithPublisherSessions(1),
		amqpx.WithCloseTimeout(time.Minute),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer func() {
		log.Info("closing amqp")
		assert.NoError(t, amqp.Close())
	}()

	select {
	case <-proceedChan:
		// can procees^
	case <-time.After(time.Minute):

	}

	// pause and reduce batch size and resume
	reconnectTimeout := 2 * time.Minute

	pauseCtx, cancel := context.WithTimeout(cctx, reconnectTimeout)
	err = handler.Pause(pauseCtx)
	cancel()
	if !assert.NoError(t, err) {
		return
	}

	handler.SetMaxBatchSize(finalBatchSize)

	time.Sleep(2 * time.Second)

	resumeCtx, cancel := context.WithTimeout(cctx, reconnectTimeout)
	err = handler.Resume(resumeCtx)
	cancel()
	if !assert.NoError(t, err) {
		return
	}

	// allow processing without nacks

	// await for subscriber to consume all messages before finishing test
	publishFinishTimeout := time.Duration(publish) * time.Second // max one second per message
	select {
	case <-time.After(publishFinishTimeout):
		t.Errorf("timeout after %s", publishFinishTimeout)
		return
	case <-processingFinshed:
		log.Info("processing finished successfully")
	}
}

// This test tests that the requeued messages preserve their order until the requeue limit is reached.
// IMPORTANT: This test requires RabbitMQ 4.0 or higher, as it uses the x-arguments to set the requeue limit.
// TODO: we loose message 0 and message 1 on ubuntu tests
// we get messages 2, 3, 4, 5, 6, 7, 8, 9
func TestRequeueLimitPreserveOrderOK(t *testing.T) {
	// we test that an event is requeued to the front of the queue as many times as it was defined.
	// https://www.rabbitmq.com/docs/quorum-queues#repeated-requeues
	t.Parallel()
	var (
		log = testlogger.NewTestLogger(t, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		})
		cctx, cancel      = context.WithCancel(context.TODO())
		funcName          = testutils.FuncName()
		nextExchangeQueue = testutils.NewExchangeQueueGenerator(funcName)
		eq1               = nextExchangeQueue()
	)

	defer cancel()

	var (
		publish                = 10 // explicitly this combination of values causes the out of order issue of the batch handler
		initialBatchSize       = 2
		subscriberFlushTimeout = 500 * time.Millisecond
		finalBatchSize         = 1

		// in case that this value is changed to a higher value, the test will fail, which is when the number of allowed requeues is exceeded.
		// aftet that requeue limit is reached, messages are not returned back to the front of the queue but at the end.
		// This prevents the raft log from growing indefinitely.
		redeliveryLimit        = types.DefaultQueueDeliveryLimit
		nackProcessingFinished = make(chan struct{})
		processingFinshed      = make(chan struct{})
	)

	// create publisher, create topology & publish messages
	pub := amqpx.New()

	pub.RegisterTopologyCreator(createTopology(log, eq1))
	pub.RegisterTopologyDeleter(deleteTopology(log, eq1))

	err := pub.Start(
		cctx,
		testutils.HealthyConnectURL,
		amqpx.WithLogger(log),
		amqpx.WithPublisherConnections(1),
		amqpx.WithPublisherSessions(1),
		amqpx.WithCloseTimeout(time.Minute),
	)
	if !assert.NoError(t, err) {
		return
	}
	defer func() {
		log.Info("closing publisher")
		assert.NoError(t, pub.Close())
	}()

	for i := 0; i < publish; i++ {
		err := pub.Publish(cctx, eq1.Exchange, eq1.RoutingKey, types.Publishing{
			ContentType: "text/plain",
			Body:        []byte(eq1.NextPubMsg()),
		})
		if !assert.NoError(t, err) {
			return
		}
	}

	time.Sleep(2 * time.Second)

	// create subscriber & start consuming messages but fail to process them which results in them being requeued
	sub := amqpx.New()

	redeliveryCounter := 0
	var once sync.Once
	handler := sub.RegisterBatchHandler(eq1.Queue, func(bctx context.Context, msgs []types.Delivery) (err error) {

		redeliveryCounter++

		if redeliveryCounter >= redeliveryLimit-2 { // only -2 is needed, but for debugging purposes in the fronend we add another -1

			once.Do(func() {
				close(nackProcessingFinished)
			})

			log.Info("blocking processing")
			<-bctx.Done()
			log.Info("unblocking processing")

			// INFO: additional redelivery once the context is canceled
			return bctx.Err() // reject & requeue, additional redelivery that is still supposed to have the correct order.

		}

		return fmt.Errorf("requeue %d messages", len(msgs))
	},
		pool.WithMaxBatchSize(initialBatchSize),
		pool.WithBatchFlushTimeout(subscriberFlushTimeout),
	)

	err = sub.Start(
		cctx,
		testutils.HealthyConnectURL,
		amqpx.WithLogger(log),
		amqpx.WithPublisherConnections(0), // subscriber only pool
		amqpx.WithPublisherSessions(0),    // subscriber only pool
		amqpx.WithCloseTimeout(time.Minute),
	)
	if !assert.NoError(t, err) {
		return
	}
	defer func() {
		log.Info("closing subscriber")
		assert.NoError(t, sub.Close())
	}()

	select {
	case <-nackProcessingFinished:
		log.Info("nack processing finished")
	case <-time.After(10 * time.Second):
		t.Errorf("nack test timeout after 10 seconds")
		return
	}

	// pause, reduce batch size and resume and start processing the messages properly
	reconnectTimeout := 2 * time.Minute

	pauseCtx, cancel := context.WithTimeout(cctx, reconnectTimeout)
	err = handler.Pause(pauseCtx)
	cancel()
	if !assert.NoError(t, err) {
		return
	}

	time.Sleep(1 * time.Second)

	var cnt int64 = 0
	handler.SetMaxBatchSize(finalBatchSize)

	// INFO: additional redilivery for final consumption
	handler.SetHandlerFunc(func(hctx context.Context, msgs []types.Delivery) error {
		select {
		case <-hctx.Done():
			return fmt.Errorf("handler context canceled before processing: %w", hctx.Err())
		default:
			// process
		}

		// At this point the order is NOT is not supposed to be broken.
		// The bigger the batch, the more data we loose
		for _, msg := range msgs {
			log.Info(fmt.Sprintf("received message: %s", string(msg.Body)))
			err := eq1.ValidateNextSubMsg(string(msg.Body))
			assert.NoError(t, err)
		}

		if atomic.AddInt64(&cnt, int64(len(msgs))) == int64(publish) {
			close(processingFinshed)
		}

		return nil
	})

	resumeCtx, cancel := context.WithTimeout(cctx, reconnectTimeout)
	err = handler.Resume(resumeCtx)
	cancel()
	if !assert.NoError(t, err) {
		return
	}

	// await for subscriber to consume all messages before finishing test
	publishFinishTimeout := time.Duration(publish) * time.Second // max one second per message
	select {
	case <-time.After(publishFinishTimeout):
		close(processingFinshed)
		<-processingFinshed
		t.Errorf("ack timeout after %s: received %d / %d messages", publishFinishTimeout, cnt, publish)
	case <-processingFinshed:
		log.Info("processing finished successfully")
	}
}

func TestHandlerPauseAndResume(t *testing.T) {
	t.Parallel()
	var wg sync.WaitGroup
	defer wg.Wait()

	iterations := 1000
	closeTimeout := max(time.Duration(iterations/3)*time.Second, 10*time.Second)
	wg.Add(iterations)
	for i := 0; i < iterations; i++ {
		go func(i int) {
			defer wg.Done()
			testHandlerPauseAndResume(t, i, closeTimeout)
		}(i)
	}
}

func testHandlerPauseAndResume(t *testing.T, i int, closeTimeout time.Duration) {
	var (
		amqp              = amqpx.New()
		log               = testlogger.NewTestLogger(t)
		cctx, cancel      = context.WithCancel(context.TODO())
		funcName          = fmt.Sprintf("%s-%d", testutils.CallerFuncName(), i)
		nextExchangeQueue = testutils.NewExchangeQueueGenerator(funcName)
		eq1               = nextExchangeQueue()
		eq2               = nextExchangeQueue()
		eq3               = nextExchangeQueue()
	)
	defer cancel()
	defer func() {
		log.Info("closing amqp")
		assert.NoError(t, amqp.Close())
	}()

	options := []amqpx.Option{
		amqpx.WithLogger(slog.New(slog.DiscardHandler)),
		amqpx.WithPublisherConnections(1),
		amqpx.WithPublisherSessions(5),
		amqpx.WithCloseTimeout(closeTimeout),
	}

	amqpPub := amqpx.New()
	amqpPub.RegisterTopologyCreator(createTopology(log, eq1, eq2, eq3))
	amqpPub.RegisterTopologyDeleter(deleteTopology(log, eq1, eq2, eq3))
	defer func() {
		assert.NoError(t, amqpPub.Close())
	}()

	err := amqpPub.Start(
		cctx,
		testutils.HealthyConnectURL,
		append(options, amqpx.WithName(funcName+"-pub"))...,
	)

	if assert.NoError(t, err) {
		return
	}

	var (
		publish = 500
		cnt     = 0
	)

	// fill queue with messages
	for i := 0; i < publish; i++ {
		err := amqpPub.Publish(cctx, eq1.Exchange, eq1.RoutingKey, types.Publishing{
			ContentType: "text/plain",
			Body:        []byte(eq1.NextPubMsg()),
		})
		if err != nil {
			assert.NoError(t, err)
			return
		}
	}

	// step 2 - process messages, pause, wait, resume, process rest, cancel context
	handler01 := amqp.RegisterHandler(eq1.Queue, func(_ context.Context, msg types.Delivery) (err error) {
		assert.Equal(t, eq1.NextSubMsg(), string(msg.Body))

		cnt++
		if cnt == publish/3 || cnt == publish/3*2 {
			err = amqp.Publish(cctx, eq2.Exchange, eq2.RoutingKey, types.Publishing{
				ContentType: "text/plain",
				Body:        []byte(eq2.NextPubMsg()),
			})
			assert.NoError(t, err)
		}

		return nil
	})

	running := true
	amqp.RegisterHandler(eq2.Queue, func(_ context.Context, msg types.Delivery) (err error) {
		assert.Equal(t, eq2.NextSubMsg(), string(msg.Body))
		log.Info(fmt.Sprintf("received toggle request: %s", string(msg.Body)))
		queue := handler01.Queue()

		if running {
			running = false

			assertActive(t, handler01, true)

			err = handler01.Pause(cctx)
			assert.NoError(t, err)

			assertActive(t, handler01, false)
		} else {
			running = true

			assertActive(t, handler01, false)

			err = handler01.Resume(cctx)
			assert.NoError(t, err)
			log.Info(fmt.Sprintf("resumed processing of %s", queue))

			assertActive(t, handler01, true)

			// trigger cancelation
			err = amqpPub.Publish(cctx, eq3.Exchange, eq3.RoutingKey, types.Publishing{
				ContentType: "text/plain",
				Body:        []byte(eq3.NextPubMsg()),
			})
			assert.NoError(t, err)
		}
		return nil
	})

	var once sync.Once
	amqp.RegisterHandler(eq3.Queue, func(_ context.Context, msg types.Delivery) (err error) {
		once.Do(func() {

			log.Info("pausing handler")
			assertActive(t, handler01, true)
			err = handler01.Pause(cctx)
			if err != nil {
				assert.NoError(t, err)
				return
			}
			assertActive(t, handler01, false)

			go func() {
				// delay cancelation (due to ack)
				time.Sleep(3 * time.Second)
				cancel()
			}()
		})
		return nil
	})

	assertActive(t, handler01, false)

	err = amqp.Start(
		cctx,
		testutils.HealthyConnectURL,
		append(options, amqpx.WithName(funcName))...,
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}

	// will be canceled when the event has reache dthe third handler
	<-cctx.Done()
	log.Info("context canceled, closing test.")
	assert.NoError(t, amqp.Close())
	assertActive(t, handler01, false)
}

func TestBatchHandlerPauseAndResume(t *testing.T) {
	t.Parallel()
	var wg sync.WaitGroup
	defer wg.Wait()
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			testBatchHandlerPauseAndResume(t, i)
		}(i)
	}
}

func testBatchHandlerPauseAndResume(t *testing.T, i int) {
	t.Logf("iteration %d", i)

	var (
		amqp              = amqpx.New()
		log               = testlogger.NewTestLogger(t)
		cctx, cancel      = context.WithCancel(context.TODO())
		funcName          = fmt.Sprintf("%s-%d", testutils.CallerFuncName(), i)
		nextExchangeQueue = testutils.NewExchangeQueueGenerator(funcName)
		eq1               = nextExchangeQueue()
		eq2               = nextExchangeQueue()
		eq3               = nextExchangeQueue()
	)
	defer func() {
		assert.NoError(t, amqp.Close())
	}()
	defer cancel()

	options := []amqpx.Option{
		amqpx.WithLogger(slog.New(slog.DiscardHandler)),
		amqpx.WithPublisherConnections(1),
		amqpx.WithPublisherSessions(5),
		amqpx.WithCloseTimeout(time.Minute),
	}

	amqpxPublish := amqpx.New()
	amqpxPublish.RegisterTopologyCreator(createTopology(log, eq1, eq2, eq3))
	amqpxPublish.RegisterTopologyDeleter(deleteTopology(log, eq1, eq2, eq3))

	err := amqpxPublish.Start(
		cctx,
		testutils.HealthyConnectURL,
		append(options, amqpx.WithName(funcName+"-pub"))...,
	)
	if !assert.NoError(t, err) {
		return
	}
	defer func() {
		assert.NoError(t, amqpxPublish.Close())
	}()

	var (
		publish = 500
		cnt     = 0
	)

	// step 1 - fill queue with messages
	// fill queue with messages
	for i := 0; i < publish; i++ {
		err := amqpxPublish.Publish(cctx, eq1.Exchange, eq1.RoutingKey, types.Publishing{
			ContentType: "text/plain",
			Body:        []byte(eq1.NextPubMsg()),
		})
		if err != nil {
			assert.NoError(t, err)
			return
		}
	}

	// step 2 - process messages, pause, wait, resume, process rest, cancel context
	handler01 := amqp.RegisterBatchHandler(eq1.Queue, func(_ context.Context, msgs []types.Delivery) (err error) {
		for _, msg := range msgs {
			assert.NoError(t, eq1.ValidateNextSubMsg(string(msg.Body)))
			cnt++
			if cnt == publish/3 || cnt == publish/3*2 {
				err = amqp.Publish(cctx, eq2.Exchange, eq2.RoutingKey, types.Publishing{
					ContentType: "text/plain",
					Body:        []byte(eq2.NextPubMsg()),
				})
				assert.NoError(t, err)
			}
		}
		return nil
	})

	running := true
	amqp.RegisterBatchHandler(eq2.Queue, func(_ context.Context, msgs []types.Delivery) (err error) {
		queue := handler01.Queue()

		for _, msg := range msgs {
			log.Info(fmt.Sprintf("received toggle request: %s", string(msg.Body)))
			if running {
				running = false

				assertActive(t, handler01, true)

				err = handler01.Pause(cctx)
				assert.NoError(t, err)

				assertActive(t, handler01, false)
			} else {
				running = true

				assertActive(t, handler01, false)

				err = handler01.Resume(cctx)
				assert.NoError(t, err)
				log.Info(fmt.Sprintf("resumed processing of %s", queue))

				assertActive(t, handler01, true)

				// trigger cancelation
				err = amqpxPublish.Publish(cctx, eq3.Exchange, eq3.RoutingKey, types.Publishing{
					ContentType: "text/plain",
					Body:        []byte(eq3.NextPubMsg()),
				})
				assert.NoError(t, err)
			}
		}
		return nil
	})

	var once sync.Once
	amqp.RegisterBatchHandler(eq3.Queue, func(_ context.Context, msgs []types.Delivery) (err error) {
		_ = msgs[0]
		once.Do(func() {

			assertActive(t, handler01, true)
			err = handler01.Pause(cctx)
			if err != nil {
				assert.NoError(t, err)
				return
			}
			assertActive(t, handler01, false)

			go func() {
				// delay cancelation (due to ack)
				time.Sleep(3 * time.Second)
				cancel()
			}()
		})

		return nil
	})

	assertActive(t, handler01, false)

	err = amqp.Start(
		cctx,
		testutils.HealthyConnectURL,
		append(options, amqpx.WithName(funcName))...,
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}

	// will be canceled when the event has reache dthe third handler
	<-cctx.Done()
	log.Info("context canceled, closing test.")
	assert.NoError(t, amqp.Close())
	assertActive(t, handler01, false)
}

func TestQueueDeletedConsumerReconnect(t *testing.T) {
	t.Parallel()
	var (
		err           error
		amqp          = amqpx.New()
		log           = testlogger.NewTestLogger(t)
		cctx, cancel  = context.WithCancel(context.TODO())
		funcName      = testutils.FuncName()
		nextQueueName = testutils.QueueNameGenerator(funcName)
		queueName     = nextQueueName()
	)
	defer cancel()
	defer func() {
		assert.NoError(t, amqp.Close())
	}()

	ts, closer := newTransientSession(t, cctx, testutils.HealthyConnectURL)
	defer closer()

	// step 1 - fill queue with messages
	amqp.RegisterTopologyCreator(func(ctx context.Context, t *pool.Topologer) error {
		_, err := t.QueueDeclare(ctx, queueName)
		if err != nil {
			return err
		}
		return nil
	})
	amqp.RegisterTopologyDeleter(func(ctx context.Context, t *pool.Topologer) error {
		_, err := t.QueueDelete(ctx, queueName)
		if err != nil {
			return err
		}
		return nil
	})

	h := amqp.RegisterHandler(queueName, func(ctx context.Context, msg types.Delivery) (err error) {
		return nil
	})

	assertActive(t, h, false)

	err = amqp.Start(
		cctx,
		testutils.HealthyConnectURL,
		amqpx.WithName(funcName),
		amqpx.WithLogger(log),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}

	assert.NoError(t, h.Pause(cctx))
	assertActive(t, h, false)

	_, err = ts.QueueDelete(cctx, queueName)
	assert.NoError(t, err)

	tctx, tcancel := context.WithTimeout(cctx, 10*time.Second)
	err = h.Resume(tctx)
	tcancel()
	assert.Error(t, err)
	assertActive(t, h, false)

	_, err = ts.QueueDeclare(cctx, queueName)
	assert.NoError(t, err)

	tctx, tcancel = context.WithTimeout(cctx, 10*time.Second)
	err = h.Resume(tctx)
	tcancel()
	assert.NoError(t, err)
	assertActive(t, h, true)
}

func TestQueueDeletedBatchConsumerReconnect(t *testing.T) {
	t.Parallel()
	var (
		err           error
		amqp          = amqpx.New()
		log           = testlogger.NewTestLogger(t)
		cctx, cancel  = context.WithCancel(context.TODO())
		funcName      = testutils.FuncName()
		nextQueueName = testutils.QueueNameGenerator(funcName)
		queueName     = nextQueueName()
	)
	defer cancel()
	defer func() {
		assert.NoError(t, amqp.Close())
	}()

	ts, closer := newTransientSession(t, cctx, testutils.HealthyConnectURL)
	defer closer()

	// step 1 - fill queue with messages
	amqp.RegisterTopologyCreator(func(ctx context.Context, t *pool.Topologer) error {
		_, err := t.QueueDeclare(ctx, queueName)
		if err != nil {
			return err
		}
		return nil
	})
	amqp.RegisterTopologyDeleter(func(ctx context.Context, t *pool.Topologer) error {
		_, err := t.QueueDelete(ctx, queueName)
		if err != nil {
			return err
		}
		return nil
	})

	h := amqp.RegisterBatchHandler(queueName, func(ctx context.Context, msg []types.Delivery) (err error) {
		return nil
	})

	assertActive(t, h, false)

	err = amqp.Start(
		cctx,
		testutils.HealthyConnectURL,
		amqpx.WithName(funcName),
		amqpx.WithLogger(log),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}

	assert.NoError(t, h.Pause(cctx))
	assertActive(t, h, false)

	_, err = ts.QueueDelete(cctx, queueName)
	assert.NoError(t, err)

	tctx, tcancel := context.WithTimeout(cctx, 5*time.Second)
	err = h.Resume(tctx)
	tcancel()
	assert.Error(t, err)
	assertActive(t, h, false)

	_, err = ts.QueueDeclare(cctx, queueName)
	assert.NoError(t, err)

	tctx, tcancel = context.WithTimeout(cctx, 5*time.Second)
	err = h.Resume(tctx)
	tcancel()
	assert.NoError(t, err)
	assertActive(t, h, true)
}

func newTransientSession(t *testing.T, ctx context.Context, connectUrl string) (session *types.Session, closer func()) {
	p, err := pool.New(ctx, connectUrl, 1, 1, pool.WithLogger(testlogger.NewTestLogger(t)))
	require.NoError(t, err)

	s, err := p.GetSession(ctx)
	require.NoError(t, err)

	return s, func() {
		p.ReturnSession(s, nil)
		err = s.Close()
		assert.NoError(t, err)
		p.Close()
		assert.NoError(t, err)
	}
}

type handlerStats interface {
	Queue() string
	IsActive(ctx context.Context) (active bool, err error)
}

func TestHandlerReset(t *testing.T) {
	t.Parallel()
	for i := 0; i < 5; i++ {
		testHandlerReset(t, i)
	}
	t.Log("done")
}

func testHandlerReset(t *testing.T, i int) {
	var (
		err               error
		amqp              = amqpx.New()
		log               = testlogger.NewTestLogger(t)
		cctx, cancel      = context.WithCancel(context.TODO())
		funcName          = fmt.Sprintf("%s-i-%d", testutils.CallerFuncName(), i)
		nextExchangeQueue = testutils.NewExchangeQueueGenerator(funcName)
		eq1               = nextExchangeQueue()
		eq2               = nextExchangeQueue()
		eq3               = nextExchangeQueue()
	)
	defer func() {
		assert.NoError(t, amqp.Close())
	}()
	defer cancel()

	options := []amqpx.Option{
		amqpx.WithLogger(slog.New(slog.DiscardHandler)),
		amqpx.WithPublisherConnections(1),
		amqpx.WithPublisherSessions(5),
		amqpx.WithCloseTimeout(time.Minute),
	}

	amqpxPublish := amqpx.New()
	amqpxPublish.RegisterTopologyCreator(createTopology(log, eq1, eq2, eq3))
	err = amqpxPublish.Start(
		cctx,
		testutils.HealthyConnectURL,
		append(options, amqpx.WithName(funcName+"-pub"))...,
	)
	require.NoError(t, err)

	var (
		publish = 50
		cnt     = 0
	)

	// step 1 - fill queue with messages
	amqp.RegisterTopologyDeleter(deleteTopology(log, eq1, eq2, eq3))

	// fill queue with messages
	for i := 0; i < publish; i++ {
		err := amqpxPublish.Publish(cctx, eq1.Exchange, eq1.RoutingKey, types.Publishing{
			ContentType: "text/plain",
			Body:        []byte(eq1.NextPubMsg()),
		})
		if err != nil {
			assert.NoError(t, err)
			return
		}
	}

	done := make(chan struct{})
	// step 2 - process messages, pause, wait, resume, process rest, cancel context
	handler01 := amqp.RegisterHandler(eq1.Queue, func(_ context.Context, msgs types.Delivery) (err error) {
		cnt++
		if cnt == publish {
			close(done)
		}
		return nil
	})

	assertActive(t, handler01, false)

	err = amqp.Start(
		cctx,
		testutils.HealthyConnectURL,
		append(options, amqpx.WithName(funcName))...,
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}

	assertActive(t, handler01, true)

	// will be canceled when the event has reached the third handler
	<-done
	cancel()

	<-cctx.Done()
	log.Info("context canceled, closing test.")
	assert.NoError(t, amqp.Close())

	// after close
	assertActive(t, handler01, false)
}

func TestBatchHandlerReset(t *testing.T) {
	t.Parallel()
	for i := 0; i < 5; i++ {
		testBatchHandlerReset(t, i)
	}
}

func testBatchHandlerReset(t *testing.T, i int) {
	var (
		err               error
		amqp              = amqpx.New()
		log               = testlogger.NewTestLogger(t)
		cctx, cancel      = context.WithCancel(context.TODO())
		funcName          = fmt.Sprintf("%s-i-%d", testutils.CallerFuncName(), i)
		nextExchangeQueue = testutils.NewExchangeQueueGenerator(funcName)
		eq1               = nextExchangeQueue()
		eq2               = nextExchangeQueue()
		eq3               = nextExchangeQueue()
	)
	defer func() {
		assert.NoError(t, amqp.Close())
	}()
	defer cancel()

	options := []amqpx.Option{
		amqpx.WithLogger(log),
		amqpx.WithPublisherConnections(1),
		amqpx.WithPublisherSessions(5),
		amqpx.WithCloseTimeout(time.Minute),
	}

	amqpxPublish := amqpx.New()
	amqpxPublish.RegisterTopologyCreator(createTopology(log, eq1, eq2, eq3))
	err = amqpxPublish.Start(
		cctx,
		testutils.HealthyConnectURL,
		append(options, amqpx.WithName(funcName+"-pub"))...,
	)
	require.NoError(t, err)

	var (
		publish = 50
		cnt     = 0
	)

	// step 1 - fill queue with messages
	amqp.RegisterTopologyDeleter(deleteTopology(log, eq1, eq2, eq3))

	// fill queue with messages
	for i := 0; i < publish; i++ {
		err := amqpxPublish.Publish(cctx, eq1.Exchange, eq1.RoutingKey, types.Publishing{
			ContentType: "text/plain",
			Body:        []byte(eq1.NextPubMsg()),
		})
		if err != nil {
			assert.NoError(t, err)
			return
		}
	}

	done := make(chan struct{})
	// step 2 - process messages, pause, wait, resume, process rest, cancel context
	handler01 := amqp.RegisterBatchHandler(eq1.Queue, func(_ context.Context, msgs []types.Delivery) (err error) {
		cnt += len(msgs)

		if cnt == publish {
			close(done)
		}
		return nil
	})

	assertActive(t, handler01, false)

	err = amqp.Start(
		cctx,
		testutils.HealthyConnectURL,
		append(options, amqpx.WithName(funcName))...,
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}

	assertActive(t, handler01, true)

	// will be canceled when the event has reached the third handler
	<-done
	cancel()

	<-cctx.Done()
	log.Info("context canceled, closing test.")
	assert.NoError(t, amqp.Close())

	// after close
	assertActive(t, handler01, false)
}

func assertActive(t *testing.T, handler handlerStats, expected bool) {
	cctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	active, err := handler.IsActive(cctx)
	if err != nil {
		assert.NoError(t, err)
		return
	}

	if expected != active {
		as := "active"
		if !expected {
			as = "inactive"
		}
		assert.Equalf(t, expected, active, "expected handler of queue %s to be %q", handler.Queue(), as)
		return
	}
}
