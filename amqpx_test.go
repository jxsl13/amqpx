package amqpx_test

import (
	"context"
	"fmt"
	"os/signal"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/jxsl13/amqpx"
	"github.com/jxsl13/amqpx/internal/testutils"
	"github.com/jxsl13/amqpx/logging"
	"github.com/jxsl13/amqpx/pool"
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
		funcName         = testutils.FuncName()
		nextExchangeName = testutils.ExchangeNameGenerator(funcName)
		exchangeName     = nextExchangeName()
	)
	defer func() {
		assert.NoError(t, amqp.Close())
	}()

	var err error
	amqp.RegisterTopologyCreator(func(ctx context.Context, t *pool.Topologer) error {
		return createExchange(ctx, exchangeName, t)
	})

	amqp.RegisterTopologyDeleter(func(ctx context.Context, t *pool.Topologer) error {
		return deleteExchange(ctx, exchangeName, t)
	})

	err = amqp.Start(
		ctx,
		testutils.HealthyConnectURL,
		amqpx.WithLogger(logging.NewTestLogger(t)),
		amqpx.WithPublisherConnections(1),
		amqpx.WithPublisherSessions(2),
	)
	assert.NoError(t, err)
}

func TestQueueDeclarePassive(t *testing.T) {
	t.Parallel()

	var (
		amqp          = amqpx.New()
		ctx           = context.TODO()
		funcName      = testutils.FuncName()
		nextQueueName = testutils.QueueNameGenerator(funcName)
		queueName     = nextQueueName()
	)
	defer func() {
		assert.NoError(t, amqp.Close())
	}()

	var err error
	amqp.RegisterTopologyCreator(func(ctx context.Context, t *pool.Topologer) error {
		return createQueue(ctx, queueName, t)
	})

	amqp.RegisterTopologyDeleter(func(ctx context.Context, t *pool.Topologer) error {
		return deleteQueue(ctx, queueName, t)
	})

	err = amqp.Start(
		ctx,
		testutils.HealthyConnectURL,
		amqpx.WithLogger(logging.NewTestLogger(t)),
		amqpx.WithPublisherConnections(1),
		amqpx.WithPublisherSessions(2),
	)
	assert.NoError(t, err)
}

func TestAMQPXPub(t *testing.T) {
	t.Parallel()

	var (
		amqp     = amqpx.New()
		ctx      = context.TODO()
		funcName = testutils.FuncName()
	)
	defer func() {
		assert.NoError(t, amqp.Close())
	}()

	amqp.RegisterTopologyCreator(createTopology(funcName))
	amqp.RegisterTopologyDeleter(deleteTopology(funcName))

	err := amqp.Start(
		ctx,
		testutils.HealthyConnectURL,
		amqpx.WithLogger(logging.NewNoOpLogger()),
		amqpx.WithPublisherConnections(1),
		amqpx.WithPublisherSessions(2),
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

	event := funcName + " - event content"

	// publish event to first queue
	err = amqp.Publish(ctx, funcName+"exchange-01", "event-01", pool.Publishing{
		ContentType: "text/plain",
		Body:        []byte(event),
	})
	if err != nil {
		assert.NoError(t, err)
		return
	}

	var (
		msg pool.Delivery
		ok  bool
	)
	for i := 0; i < 20; i++ {
		msg, ok, err = amqp.Get(ctx, funcName+"queue-01", false)
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

	assert.Equal(t, event, string(msg.Body))
}

func TestAMQPXSubAndPub(t *testing.T) {
	var (
		amqp         = amqpx.New()
		log          = logging.NewTestLogger(t)
		cctx, cancel = signal.NotifyContext(context.TODO(), syscall.SIGINT, syscall.SIGINT)
		funcName     = testutils.FuncName()
	)
	defer cancel()
	defer func() {
		log.Info("closing amqp")
		assert.NoError(t, amqp.Close())
	}()

	amqp.RegisterTopologyCreator(createTopology(funcName))
	amqp.RegisterTopologyDeleter(deleteTopology(funcName))

	eventContent := funcName + " - event content"

	amqp.RegisterHandler(funcName+"queue-01", func(ctx context.Context, msg pool.Delivery) error {
		log.Info("subscriber of queue-01")
		cancel()
		return nil
	})

	err := amqp.Start(
		cctx,
		testutils.HealthyConnectURL,
		amqpx.WithLogger(logging.NewNoOpLogger()),
		amqpx.WithPublisherConnections(1),
		amqpx.WithPublisherSessions(5),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}

	// publish event to first queue

	err = amqp.Publish(cctx, funcName+"exchange-01", "event-01", pool.Publishing{
		ContentType: "text/plain",
		Body:        []byte(eventContent),
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
		amqp     = amqpx.New()
		ctx      = context.TODO()
		funcName = testutils.FuncName()
		log      = logging.NewTestLogger(t)
	)
	defer func() {
		assert.NoError(t, amqp.Close())
	}()

	amqp.RegisterTopologyCreator(createTopology(funcName))
	amqp.RegisterTopologyDeleter(deleteTopology(funcName))

	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGINT)
	defer cancel()

	eventContent := funcName + " - event content"

	// publish -> queue-01 -> subscriber-01 -> queue-02 -> subscriber-02 -> queue-03 -> subscriber-03 -> cancel context
	amqp.RegisterHandler(funcName+"queue-01", func(ctx context.Context, msg pool.Delivery) error {
		log.Info("handler of subscriber-01")

		err := amqp.Publish(ctx, funcName+"exchange-02", "event-02", pool.Publishing{
			ContentType: msg.ContentType,
			Body:        msg.Body,
		})

		if err != nil {
			log.Error("subscriber-01:", err)
			assert.NoError(t, err)
		}

		return nil
	},
		pool.ConsumeOptions{ConsumerTag: funcName + "subscriber-01"},
	)

	amqp.RegisterHandler(funcName+"queue-02", func(ctx context.Context, msg pool.Delivery) error {
		log.Info("handler of subscriber-02")

		err := amqp.Publish(ctx, funcName+"exchange-03", "event-03", pool.Publishing{
			ContentType: msg.ContentType,
			Body:        msg.Body,
		})

		if err != nil {
			log.Error("subscriber-02:", err)
		}

		return nil
	}, pool.ConsumeOptions{ConsumerTag: funcName + "subscriber-02"})

	amqp.RegisterHandler(funcName+"queue-03", func(ctx context.Context, msg pool.Delivery) error {
		log.Info("handler of subscriber-03: canceling context!")
		cancel()
		return nil
	}, pool.ConsumeOptions{ConsumerTag: funcName + "subscriber-03"})

	err := amqp.Start(
		ctx,
		testutils.HealthyConnectURL,
		amqpx.WithLogger(log),
		amqpx.WithPublisherConnections(1),
		amqpx.WithPublisherSessions(5),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}

	// publish event to first queue

	err = amqp.Publish(ctx, funcName+"exchange-01", "event-01", pool.Publishing{
		ContentType: "text/plain",
		Body:        []byte(eventContent),
	})
	if err != nil {
		assert.NoError(t, err)
		return
	}

	// will be canceled when the event has reache dthe third handler
	<-ctx.Done()
	log.Info("context canceled, closing test.")
}

func TestAMQPXSubHandler(t *testing.T) {
	t.Parallel()

	var (
		amqp     = amqpx.New()
		ctx      = context.TODO()
		funcName = testutils.FuncName()
		log      = logging.NewTestLogger(t)
	)
	defer func() {
		assert.NoError(t, amqp.Close())
	}()

	amqp.RegisterTopologyCreator(createTopology(funcName))
	amqp.RegisterTopologyDeleter(deleteTopology(funcName))

	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGINT)
	defer cancel()

	eventContent := funcName + " - event content"

	amqp.RegisterHandler(funcName+"queue-01", func(ctx context.Context, msg pool.Delivery) error {
		log.Info("subscriber of queue-01")
		assert.Equal(t, eventContent, string(msg.Body))
		cancel()
		return nil
	})

	err := amqp.Start(
		ctx,
		testutils.HealthyConnectURL,
		amqpx.WithLogger(logging.NewNoOpLogger()),
		amqpx.WithPublisherConnections(1),
		amqpx.WithPublisherSessions(5),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}

	// publish event to first queue

	err = amqp.Publish(ctx, funcName+"exchange-01", "event-01", pool.Publishing{
		ContentType: "text/plain",
		Body:        []byte(eventContent),
	})
	if err != nil {
		assert.NoError(t, err)
		return
	}

	// will be canceled when the event has reache dthe third handler
	<-ctx.Done()
	log.Info("context canceled, closing test.")
}

func TestCreateDeleteTopology(t *testing.T) {
	t.Parallel()

	var (
		amqp     = amqpx.New()
		ctx      = context.TODO()
		funcName = testutils.FuncName()
		log      = logging.NewTestLogger(t)
	)
	defer func() {
		assert.NoError(t, amqp.Close())
	}()

	amqp.RegisterTopologyCreator(createTopology(funcName))
	amqp.RegisterTopologyDeleter(deleteTopology(funcName))

	err := amqp.Start(
		ctx,
		testutils.HealthyConnectURL,
		amqpx.WithLogger(log),
		amqpx.WithPublisherConnections(1),
		amqpx.WithPublisherSessions(2),
	)
	assert.NoError(t, err)
}

func TestPauseResumeHandlerNoProcessing(t *testing.T) {
	t.Parallel()

	var (
		err           error
		amqp          = amqpx.New()
		cctx, cancel  = signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGINT)
		funcName      = testutils.FuncName()
		log           = logging.NewTestLogger(t)
		nextQueueName = testutils.QueueNameGenerator(funcName)
		queueName     = nextQueueName()
	)
	defer func() {
		assert.NoError(t, amqp.Close())
	}()

	defer cancel()

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

	handler := amqp.RegisterHandler(queueName, func(ctx context.Context, d pool.Delivery) error {
		log.Info("received message")
		return nil
	})

	err = amqp.Start(
		cctx,
		testutils.HealthyConnectURL,
		amqpx.WithLogger(logging.NewNoOpLogger()),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer func() {
		err = amqp.Close()
		assert.NoError(t, err)
	}()

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

func TestHandlerPauseAndResume(t *testing.T) {
	for i := 0; i < 10; i++ {
		testHandlerPauseAndResume(t, i)
	}
}

func testHandlerPauseAndResume(t *testing.T, i int) {
	t.Logf("iteration %d", i)

	var (
		err          error
		amqp         = amqpx.New()
		cctx, cancel = signal.NotifyContext(context.TODO(), syscall.SIGINT, syscall.SIGINT)
		funcName     = testutils.FuncName(3) + fmt.Sprintf("-i-%d", i)
		log          = logging.NewTestLogger(t)
	)
	defer cancel()

	options := []amqpx.Option{
		amqpx.WithLogger(logging.NewNoOpLogger()),
		amqpx.WithPublisherConnections(1),
		amqpx.WithPublisherSessions(5),
	}

	amqpxPublish := amqpx.New()
	amqpxPublish.RegisterTopologyCreator(createTopology(funcName))

	eventContent := "TestHandlerPauseAndResume - event content"

	var (
		publish = 500
		cnt     = 0
	)

	// step 1 - fill queue with messages
	amqp.RegisterTopologyDeleter(deleteTopology(funcName))
	err = amqpxPublish.Start(
		cctx,
		funcName,
		options...,
	)
	require.NoError(t, err)

	// fill queue with messages
	for i := 0; i < publish; i++ {
		err := amqpxPublish.Publish(cctx, funcName+"exchange-01", "event-01", pool.Publishing{
			ContentType: "text/plain",
			Body:        []byte(fmt.Sprintf("%s: message number %d", eventContent, i)),
		})
		if err != nil {
			assert.NoError(t, err)
			return
		}
	}

	// step 2 - process messages, pause, wait, resume, process rest, cancel context
	handler01 := amqp.RegisterHandler(funcName+"queue-01", func(ctx context.Context, msg pool.Delivery) (err error) {
		cnt++
		if cnt == publish/3 || cnt == publish/3*2 {
			err = amqp.Publish(ctx, funcName+"exchange-02", "event-02", pool.Publishing{
				ContentType: "text/plain",
				Body:        []byte(fmt.Sprintf("%s: hit %d messages, toggling processing", eventContent, cnt)),
			})
			assert.NoError(t, err)
		}

		return nil
	})

	running := true
	amqp.RegisterHandler(funcName+"queue-02", func(ctx context.Context, msg pool.Delivery) (err error) {
		log.Infof("received toggle request: %s", string(msg.Body))
		queue := handler01.Queue()

		if running {
			running = false

			assertActive(t, handler01, true)

			err = handler01.Pause(context.Background())
			assert.NoError(t, err)

			assertActive(t, handler01, false)
		} else {
			running = true

			assertActive(t, handler01, false)

			err = handler01.Resume(context.Background())
			assert.NoError(t, err)
			log.Infof("resumed processing of %s", queue)

			assertActive(t, handler01, true)

			// trigger cancelation
			err = amqpxPublish.Publish(ctx, funcName+"exchange-03", "event-03", pool.Publishing{
				ContentType: "text/plain",
				Body:        []byte(fmt.Sprintf("%s: delayed toggle back", eventContent)),
			})
			assert.NoError(t, err)
		}
		return nil
	})

	var once sync.Once
	amqp.RegisterHandler(funcName+"queue-03", func(ctx context.Context, msg pool.Delivery) (err error) {
		once.Do(func() {

			log.Info("pausing handler")
			assertActive(t, handler01, true)
			err = handler01.Pause(ctx)
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
		amqpx.WithLogger(logging.NewNoOpLogger()),
		amqpx.WithPublisherConnections(1),
		amqpx.WithPublisherSessions(5),
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
	for i := 0; i < 10; i++ {
		testBatchHandlerPauseAndResume(t, i)
	}
}

func testBatchHandlerPauseAndResume(t *testing.T, i int) {
	t.Logf("iteration %d", i)

	var (
		err          error
		amqp         = amqpx.New()
		cctx, cancel = signal.NotifyContext(context.TODO(), syscall.SIGINT, syscall.SIGINT)
		funcName     = testutils.FuncName(3) + fmt.Sprintf("-i-%d", i)
		log          = logging.NewTestLogger(t)
	)
	defer cancel()

	options := []amqpx.Option{
		amqpx.WithLogger(logging.NewNoOpLogger()),
		amqpx.WithPublisherConnections(1),
		amqpx.WithPublisherSessions(5),
	}

	amqpxPublish := amqpx.New()
	amqpxPublish.RegisterTopologyCreator(createTopology(funcName))
	err = amqpxPublish.Start(
		cctx,
		testutils.HealthyConnectURL,
		options...,
	)
	require.NoError(t, err)

	eventContent := funcName + " - event content"

	var (
		publish = 500
		cnt     = 0
	)

	// step 1 - fill queue with messages
	amqp.RegisterTopologyDeleter(deleteTopology(funcName))

	// fill queue with messages
	for i := 0; i < publish; i++ {
		err := amqpxPublish.Publish(cctx, funcName+"exchange-01", "event-01", pool.Publishing{
			ContentType: "text/plain",
			Body:        []byte(fmt.Sprintf("%s: message number %d", eventContent, i)),
		})
		if err != nil {
			assert.NoError(t, err)
			return
		}
	}

	// step 2 - process messages, pause, wait, resume, process rest, cancel context
	handler01 := amqp.RegisterBatchHandler(funcName+"queue-01", func(ctx context.Context, msgs []pool.Delivery) (err error) {
		for _, msg := range msgs {
			cnt++
			if cnt == publish/3 || cnt == publish/3*2 {
				err = amqp.Publish(ctx, funcName+"exchange-02", "event-02", pool.Publishing{
					ContentType: "text/plain",
					Body:        []byte(fmt.Sprintf("%s: hit %d messages, toggling processing: %s", eventContent, cnt, string(msg.Body))),
				})
				assert.NoError(t, err)
			}
		}
		return nil
	})

	running := true
	amqp.RegisterBatchHandler(funcName+"queue-02", func(ctx context.Context, msgs []pool.Delivery) (err error) {
		queue := handler01.Queue()

		for _, msg := range msgs {
			log.Infof("received toggle request: %s", string(msg.Body))
			if running {
				running = false

				assertActive(t, handler01, true)

				err = handler01.Pause(ctx)
				assert.NoError(t, err)

				assertActive(t, handler01, false)
			} else {
				running = true

				assertActive(t, handler01, false)

				err = handler01.Resume(ctx)
				assert.NoError(t, err)
				log.Infof("resumed processing of %s", queue)

				assertActive(t, handler01, true)

				// trigger cancelation
				err = amqpxPublish.Publish(ctx, funcName+"exchange-03", "event-03", pool.Publishing{
					ContentType: "text/plain",
					Body:        []byte(fmt.Sprintf("%s: delayed toggle back", eventContent)),
				})
				assert.NoError(t, err)
			}
		}
		return nil
	})

	var once sync.Once
	amqp.RegisterBatchHandler(funcName+"queue-03", func(ctx context.Context, msgs []pool.Delivery) (err error) {
		_ = msgs[0]
		once.Do(func() {

			assertActive(t, handler01, true)
			err = handler01.Pause(ctx)
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
		amqpx.WithLogger(logging.NewNoOpLogger()),
		amqpx.WithPublisherConnections(1),
		amqpx.WithPublisherSessions(5),
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
	var (
		err           error
		amqp          = amqpx.New()
		log           = logging.NewTestLogger(t)
		cctx, cancel  = signal.NotifyContext(context.TODO(), syscall.SIGINT, syscall.SIGINT)
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

	h := amqp.RegisterHandler(queueName, func(ctx context.Context, msg pool.Delivery) (err error) {
		return nil
	})

	assertActive(t, h, false)

	err = amqp.Start(
		cctx,
		testutils.HealthyConnectURL,
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

func TestQueueDeletedBatchConsumerReconnect(t *testing.T) {
	var (
		err           error
		amqp          = amqpx.New()
		log           = logging.NewTestLogger(t)
		cctx, cancel  = signal.NotifyContext(context.TODO(), syscall.SIGINT, syscall.SIGINT)
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

	h := amqp.RegisterBatchHandler(queueName, func(ctx context.Context, msg []pool.Delivery) (err error) {
		return nil
	})

	assertActive(t, h, false)

	err = amqp.Start(
		cctx,
		testutils.HealthyConnectURL,
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

func newTransientSession(t *testing.T, ctx context.Context, connectUrl string) (session *pool.Session, closer func()) {
	p, err := pool.New(ctx, connectUrl, 1, 1, pool.WithLogger(logging.NewTestLogger(t)))
	require.NoError(t, err)

	s, err := p.GetSession(ctx)
	require.NoError(t, err)

	return s, func() {
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
	for i := 0; i < 5; i++ {
		testHandlerReset(t, i)
	}
	t.Log("done")
}

func testHandlerReset(t *testing.T, i int) {
	var (
		err          error
		amqp         = amqpx.New()
		log          = logging.NewTestLogger(t)
		cctx, cancel = signal.NotifyContext(context.TODO(), syscall.SIGINT, syscall.SIGINT)
		funcName     = testutils.FuncName(3) + fmt.Sprintf("-i-%d", i)
	)
	defer cancel()

	options := []amqpx.Option{
		amqpx.WithLogger(logging.NewNoOpLogger()),
		amqpx.WithPublisherConnections(1),
		amqpx.WithPublisherSessions(5),
	}

	amqpxPublish := amqpx.New()
	amqpxPublish.RegisterTopologyCreator(createTopology(funcName))
	err = amqpxPublish.Start(
		cctx,
		testutils.HealthyConnectURL,
		options...,
	)
	require.NoError(t, err)

	eventContent := funcName + " - event content"

	var (
		publish = 50
		cnt     = 0
	)

	// step 1 - fill queue with messages
	amqp.RegisterTopologyDeleter(deleteTopology(funcName))

	// fill queue with messages
	for i := 0; i < publish; i++ {
		err := amqpxPublish.Publish(cctx, funcName+"exchange-01", "event-01", pool.Publishing{
			ContentType: "text/plain",
			Body:        []byte(fmt.Sprintf("%s: message number %d", eventContent, i)),
		})
		if err != nil {
			assert.NoError(t, err)
			return
		}
	}

	done := make(chan struct{})
	// step 2 - process messages, pause, wait, resume, process rest, cancel context
	handler01 := amqp.RegisterHandler(funcName+"queue-01", func(ctx context.Context, msgs pool.Delivery) (err error) {
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
		amqpx.WithLogger(logging.NewNoOpLogger()),
		amqpx.WithPublisherConnections(1),
		amqpx.WithPublisherSessions(5),
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
	for i := 0; i < 5; i++ {
		testBatchHandlerReset(t, i)
	}
	t.Log("done")
}

func testBatchHandlerReset(t *testing.T, i int) {
	var (
		err          error
		amqp         = amqpx.New()
		log          = logging.NewTestLogger(t)
		cctx, cancel = signal.NotifyContext(context.TODO(), syscall.SIGINT, syscall.SIGINT)
		funcName     = testutils.FuncName(3) + fmt.Sprintf("-i-%d", i)
	)
	defer cancel()

	options := []amqpx.Option{
		amqpx.WithLogger(logging.NewNoOpLogger()),
		amqpx.WithPublisherConnections(1),
		amqpx.WithPublisherSessions(5),
	}

	amqpxPublish := amqpx.New()
	amqpxPublish.RegisterTopologyCreator(createTopology(funcName))
	err = amqpxPublish.Start(
		cctx,
		testutils.HealthyConnectURL,
		options...,
	)
	require.NoError(t, err)

	eventContent := "TestBatchHandlerReset - event content"

	var (
		publish = 50
		cnt     = 0
	)

	// step 1 - fill queue with messages
	amqp.RegisterTopologyDeleter(deleteTopology(funcName))

	// fill queue with messages
	for i := 0; i < publish; i++ {
		err := amqpxPublish.Publish(cctx, funcName+"exchange-01", "event-01", pool.Publishing{
			ContentType: "text/plain",
			Body:        []byte(fmt.Sprintf("%s: message number %d", eventContent, i)),
		})
		if err != nil {
			assert.NoError(t, err)
			return
		}
	}

	done := make(chan struct{})
	// step 2 - process messages, pause, wait, resume, process rest, cancel context
	handler01 := amqp.RegisterBatchHandler(funcName+"queue-01", func(ctx context.Context, msgs []pool.Delivery) (err error) {
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
		amqpx.WithLogger(logging.NewNoOpLogger()),
		amqpx.WithPublisherConnections(1),
		amqpx.WithPublisherSessions(5),
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
	active, err := handler.IsActive(context.Background())
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
