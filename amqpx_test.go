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
	"github.com/jxsl13/amqpx/logging"
	"github.com/jxsl13/amqpx/pool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

var (
	connectURL = amqpx.NewURL("localhost", 5672, "admin", "password")
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(
		m,
		goleak.IgnoreTopFunction("internal/poll.runtime_pollWait"),
		goleak.IgnoreTopFunction("github.com/rabbitmq/amqp091-go.(*Connection).heartbeater"),
		goleak.IgnoreTopFunction("net/http.(*persistConn).writeLoop"),
	)
}

func TestExchangeDeclarePassive(t *testing.T) {
	log := logging.NewTestLogger(t)
	defer amqpx.Reset()

	eName := "exchange-01"
	var err error
	amqpx.RegisterTopologyCreator(func(t *pool.Topologer) error {
		return createExchange(eName, t)
	})

	amqpx.RegisterTopologyDeleter(func(t *pool.Topologer) error {
		return deleteExchange(eName, t)
	})

	err = amqpx.Start(
		connectURL,
		amqpx.WithLogger(log),
		amqpx.WithPublisherConnections(1),
		amqpx.WithPublisherSessions(2),
	)
	assert.NoError(t, err)
}

func TestQueueDeclarePassive(t *testing.T) {
	log := logging.NewTestLogger(t)
	defer amqpx.Reset()

	qName := "queue-01"
	var err error
	amqpx.RegisterTopologyCreator(func(t *pool.Topologer) error {
		return createQueue(qName, t)
	})

	amqpx.RegisterTopologyDeleter(func(t *pool.Topologer) error {
		return deleteQueue(qName, t)
	})

	err = amqpx.Start(
		connectURL,
		amqpx.WithLogger(log),
		amqpx.WithPublisherConnections(1),
		amqpx.WithPublisherSessions(2),
	)
	assert.NoError(t, err)
}

func TestAMQPXPub(t *testing.T) {
	log := logging.NewTestLogger(t)
	defer amqpx.Reset()

	amqpx.RegisterTopologyCreator(createTopology)
	amqpx.RegisterTopologyDeleter(deleteTopology)

	err := amqpx.Start(
		connectURL,
		amqpx.WithLogger(log),
		amqpx.WithPublisherConnections(1),
		amqpx.WithPublisherSessions(2),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer func() {
		// will be canceled when the event has reache dthe third handler
		err = amqpx.Close()
		assert.NoError(t, err)
	}()

	event := "TestAMQPXPub - event content"

	// publish event to first queue
	err = amqpx.Publish("exchange-01", "event-01", pool.Publishing{
		ContentType: "application/json",
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
		msg, ok, err = amqpx.Get("queue-01", false)
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
	log := logging.NewTestLogger(t)
	defer amqpx.Reset()

	amqpx.RegisterTopologyCreator(createTopology)
	amqpx.RegisterTopologyDeleter(deleteTopology)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGINT)
	defer cancel()

	eventContent := "TestAMQPXSubAndPub - event content"

	amqpx.RegisterHandler("queue-01", func(msg pool.Delivery) error {
		log.Info("subscriber of queue-01")
		cancel()
		return nil
	})

	err := amqpx.Start(
		connectURL,
		amqpx.WithLogger(log),
		amqpx.WithPublisherConnections(1),
		amqpx.WithPublisherSessions(5),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}

	// publish event to first queue

	err = amqpx.Publish("exchange-01", "event-01", pool.Publishing{
		ContentType: "application/json",
		Body:        []byte(eventContent),
	})
	if err != nil {
		assert.NoError(t, err)
		return
	}

	// will be canceled when the event has reache dthe third handler
	<-ctx.Done()
	log.Info("context canceled, closing test.")
	err = amqpx.Close()
	assert.NoError(t, err)
}

func TestAMQPXSubAndPubMulti(t *testing.T) {
	log := logging.NewTestLogger(t)
	defer amqpx.Reset()

	amqpx.RegisterTopologyCreator(createTopology)
	amqpx.RegisterTopologyDeleter(deleteTopology)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGINT)
	defer cancel()

	eventContent := "TestAMQPXSubAndPub - event content"

	// publish -> queue-01 -> subscriber-01 -> queue-02 -> subscriber-02 -> queue-03 -> subscriber-03 -> cancel context
	amqpx.RegisterHandler("queue-01", func(msg pool.Delivery) error {
		log.Info("handler of subscriber-01")

		err := amqpx.Publish("exchange-02", "event-02", pool.Publishing{
			ContentType: msg.ContentType,
			Body:        msg.Body,
		})

		if err != nil {
			log.Error("subscriber-01:", err)
		}

		return nil
	},
		pool.ConsumeOptions{ConsumerTag: "subscriber-01"},
	)

	amqpx.RegisterHandler("queue-02", func(msg pool.Delivery) error {
		log.Info("handler of subscriber-02")

		err := amqpx.Publish("exchange-03", "event-03", pool.Publishing{
			ContentType: msg.ContentType,
			Body:        msg.Body,
		})

		if err != nil {
			log.Error("subscriber-02:", err)
		}

		return nil
	}, pool.ConsumeOptions{ConsumerTag: "subscriber-02"})

	amqpx.RegisterHandler("queue-03", func(msg pool.Delivery) error {
		log.Info("handler of subscriber-03: canceling context!")
		cancel()
		return nil
	}, pool.ConsumeOptions{ConsumerTag: "subscriber-03"})

	err := amqpx.Start(
		connectURL,
		amqpx.WithLogger(log),
		amqpx.WithPublisherConnections(1),
		amqpx.WithPublisherSessions(5),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}

	// publish event to first queue

	err = amqpx.Publish("exchange-01", "event-01", pool.Publishing{
		ContentType: "application/json",
		Body:        []byte(eventContent),
	})
	if err != nil {
		assert.NoError(t, err)
		return
	}

	// will be canceled when the event has reache dthe third handler
	<-ctx.Done()
	log.Info("context canceled, closing test.")
	err = amqpx.Close()
	assert.NoError(t, err)
}

func TestAMQPXSubHandler(t *testing.T) {
	log := logging.NewTestLogger(t)
	defer amqpx.Reset()

	amqpx.RegisterTopologyCreator(createTopology)
	amqpx.RegisterTopologyDeleter(deleteTopology)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGINT)
	defer cancel()

	eventContent := "TestAMQPXSubAndPub - event content"

	amqpx.RegisterHandler("queue-01", func(msg pool.Delivery) error {
		log.Info("subscriber of queue-01")
		cancel()
		return nil
	})

	err := amqpx.Start(
		connectURL,
		amqpx.WithLogger(log),
		amqpx.WithPublisherConnections(1),
		amqpx.WithPublisherSessions(5),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}

	// publish event to first queue

	err = amqpx.Publish("exchange-01", "event-01", pool.Publishing{
		ContentType: "application/json",
		Body:        []byte(eventContent),
	})
	if err != nil {
		assert.NoError(t, err)
		return
	}

	// will be canceled when the event has reache dthe third handler
	<-ctx.Done()
	log.Info("context canceled, closing test.")
	err = amqpx.Close()
	assert.NoError(t, err)
}

func TestCreateDeleteTopology(t *testing.T) {
	log := logging.NewTestLogger(t)
	defer amqpx.Reset()

	amqpx.RegisterTopologyCreator(createTopology)
	amqpx.RegisterTopologyDeleter(deleteTopology)

	err := amqpx.Start(
		connectURL,
		amqpx.WithLogger(log),
		amqpx.WithPublisherConnections(1),
		amqpx.WithPublisherSessions(2),
	)
	assert.NoError(t, err)
}

func TestPauseResumeHandlerNoProcessing(t *testing.T) {
	queueName := "testPauseResumeHandler-01"
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGINT)
	defer cancel()

	log := logging.NewNoOpLogger()
	transientPool, err := pool.New(
		connectURL,
		1,
		1,
		pool.WithLogger(log),
		pool.WithContext(ctx),
	)
	require.NoError(t, err)
	defer transientPool.Close()

	ts, err := transientPool.GetTransientSession(ctx)
	require.NoError(t, err)
	defer func() {
		transientPool.ReturnSession(ts, false)
	}()

	amqp := amqpx.New()
	amqp.RegisterTopologyCreator(func(t *pool.Topologer) error {
		_, err := t.QueueDeclare(queueName)
		if err != nil {
			return err
		}
		return nil
	})

	amqp.RegisterTopologyDeleter(func(t *pool.Topologer) error {
		_, err := t.QueueDelete(queueName)
		if err != nil {
			return err
		}
		return nil
	})

	handler := amqp.RegisterHandler(queueName, func(d pool.Delivery) error {
		log.Info("received message")
		return nil
	})

	err = amqp.Start(connectURL, amqpx.WithLogger(log))
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer func() {
		err = amqp.Close()
		assert.NoError(t, err)
	}()

	for i := 0; i < 5; i++ {

		assertConsumers(t, ts, queueName, 1)
		assertActive(t, handler, true)

		err = handler.Pause(context.Background())
		if err != nil {
			assert.NoError(t, err)
			return
		}

		assertConsumers(t, ts, queueName, 0)
		assertActive(t, handler, false)

		err = handler.Resume(context.Background())
		if err != nil {
			assert.NoError(t, err)
			return
		}

		assertActive(t, handler, true)
		assertConsumers(t, ts, queueName, 1)
	}
}

func TestHandlerPauseAndResume(t *testing.T) {
	for i := 0; i < 5; i++ {
		testHandlerPauseAndResume(t)
	}
}

func testHandlerPauseAndResume(t *testing.T) {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGINT)
	defer cancel()

	log := logging.NewNoOpLogger()
	defer amqpx.Reset()

	options := []amqpx.Option{
		amqpx.WithLogger(log),
		amqpx.WithPublisherConnections(1),
		amqpx.WithPublisherSessions(5), // only slow close once in the transient pool
	}

	transientPool, err := pool.New(
		connectURL,
		1,
		1,
		pool.WithLogger(log),
		pool.WithContext(ctx),
	)
	require.NoError(t, err)
	defer transientPool.Close()

	ts, err := transientPool.GetSession()
	require.NoError(t, err)
	defer func() {
		transientPool.ReturnSession(ts, false)
	}()

	amqpxPublish := amqpx.New()
	amqpxPublish.RegisterTopologyCreator(createTopology)

	eventContent := "TestHandlerPauseAndResume - event content"

	var (
		publish = 5000
		cnt     = 0
	)

	// step 1 - fill queue with messages
	amqpx.RegisterTopologyDeleter(deleteTopology)
	err = amqpxPublish.Start(connectURL, options...)
	require.NoError(t, err)

	// fill queue with messages
	for i := 0; i < publish; i++ {
		err := amqpxPublish.Publish("exchange-01", "event-01", pool.Publishing{
			ContentType: "application/json",
			Body:        []byte(fmt.Sprintf("%s: message number %d", eventContent, i)),
		})
		if err != nil {
			assert.NoError(t, err)
			return
		}
	}

	// step 2 - process messages, pause, wait, resume, process rest, cancel context
	handler01 := amqpx.RegisterHandler("queue-01", func(msg pool.Delivery) (err error) {
		cnt++
		if cnt == publish/3 || cnt == publish/3*2 {
			err = amqpx.Publish("exchange-02", "event-02", pool.Publishing{
				ContentType: "application/json",
				Body:        []byte(fmt.Sprintf("%s: hit %d messages, toggling processing", eventContent, cnt)),
			})
			assert.NoError(t, err)
		}

		return nil
	})

	running := true
	amqpx.RegisterHandler("queue-02", func(msg pool.Delivery) (err error) {
		log.Infof("received toggle request: %s", string(msg.Body))
		queue := handler01.Queue()

		if running {
			running = false

			assertActive(t, handler01, true)
			assertConsumers(t, ts, queue, 1)

			err = handler01.Pause(context.Background())
			assert.NoError(t, err)

			assertActive(t, handler01, false)
			assertConsumers(t, ts, queue, 0)
		} else {
			running = true

			assertActive(t, handler01, false)
			assertConsumers(t, ts, queue, 0)

			err = handler01.Resume(context.Background())
			assert.NoError(t, err)
			log.Infof("resumed processing of %s", queue)

			assertActive(t, handler01, true)
			assertConsumers(t, ts, queue, 1)

			// trigger cancelation
			err = amqpxPublish.Publish("exchange-03", "event-03", pool.Publishing{
				ContentType: "application/json",
				Body:        []byte(fmt.Sprintf("%s: delayed toggle back", eventContent)),
			})
			assert.NoError(t, err)
		}
		return nil
	})

	amqpx.RegisterHandler("queue-03", func(msg pool.Delivery) (err error) {

		queue := handler01.Queue()

		assertActive(t, handler01, true)
		err = handler01.Pause(context.Background())
		if err != nil {
			assert.NoError(t, err)
			return nil
		}
		assertActive(t, handler01, false)

		q1, err := ts.QueueDeclarePassive(queue)
		if err != nil {
			assert.NoError(t, err)
			return nil
		}

		// wait for potential further processing
		time.Sleep(3 * time.Second)

		q2, err := ts.QueueDeclarePassive(queue)
		assert.NoError(t, err)
		if err != nil {
			return nil
		}

		assert.Equal(t, q1, q2) // message count should also be equal
		assertConsumers(t, ts, queue, 0)

		go func() {
			// delay cancelation (due to ack)
			time.Sleep(3 * time.Second)
			cancel()
		}()
		return nil
	})

	assertActive(t, handler01, false)

	err = amqpx.Start(
		connectURL,
		amqpx.WithLogger(log),
		amqpx.WithPublisherConnections(1),
		amqpx.WithPublisherSessions(5),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}

	// will be canceled when the event has reache dthe third handler
	<-ctx.Done()
	log.Info("context canceled, closing test.")
	err = amqpx.Close()
	assert.NoError(t, err)
}


func TestBatchHandlerPauseAndResume(t *testing.T) {
	for i := 0; i < 5; i++ {
		testBatchHandlerPauseAndResume(t)
	}
}

func testBatchHandlerPauseAndResume(t *testing.T) {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGINT)
	defer cancel()

	log := logging.NewNoOpLogger()
	defer amqpx.Reset()

	options := []amqpx.Option{
		amqpx.WithLogger(log),
		amqpx.WithPublisherConnections(1),
		amqpx.WithPublisherSessions(5), // only slow close once in the transient pool
	}

	transientPool, err := pool.New(
		connectURL,
		1,
		1,
		pool.WithLogger(log),
		pool.WithContext(ctx),
	)
	require.NoError(t, err)
	defer transientPool.Close()

	ts, err := transientPool.GetSession()
	require.NoError(t, err)
	defer func() {
		transientPool.ReturnSession(ts, false)
	}()

	amqpxPublish := amqpx.New()
	amqpxPublish.RegisterTopologyCreator(createTopology)

	eventContent := "TestHandlerPauseAndResume - event content"

	var (
		publish = 10000
		cnt     = 0
	)

	// step 1 - fill queue with messages
	amqpx.RegisterTopologyDeleter(deleteTopology)
	err = amqpxPublish.Start(connectURL, options...)
	require.NoError(t, err)

	// fill queue with messages
	for i := 0; i < publish; i++ {
		err := amqpxPublish.Publish("exchange-01", "event-01", pool.Publishing{
			ContentType: "application/json",
			Body:        []byte(fmt.Sprintf("%s: message number %d", eventContent, i)),
		})
		if err != nil {
			assert.NoError(t, err)
			return
		}
	}

	// step 2 - process messages, pause, wait, resume, process rest, cancel context
	handler01 := amqpx.RegisterBatchHandler("queue-01", func(msgs []pool.Delivery) (err error) {
		for _, msg := range msgs {
			cnt++
			if cnt == publish/3 || cnt == publish/3*2 {
				err = amqpx.Publish("exchange-02", "event-02", pool.Publishing{
					ContentType: "application/json",
					Body:        []byte(fmt.Sprintf("%s: hit %d messages, toggling processing: %s", eventContent, cnt, string(msg.Body))),
				})
				assert.NoError(t, err)
			}
		}
		return nil
	})

	running := true
	amqpx.RegisterBatchHandler("queue-02", func(msgs []pool.Delivery) (err error) {
		queue := handler01.Queue()

		for _, msg := range msgs {
			log.Infof("received toggle request: %s", string(msg.Body))
			if running {
				running = false

				assertActive(t, handler01, true)
				assertConsumers(t, ts, queue, 1)

				err = handler01.Pause(context.Background())
				assert.NoError(t, err)

				assertActive(t, handler01, false)
				assertConsumers(t, ts, queue, 0)
			} else {
				running = true

				assertActive(t, handler01, false)
				assertConsumers(t, ts, queue, 0)

				err = handler01.Resume(context.Background())
				assert.NoError(t, err)
				log.Infof("resumed processing of %s", queue)

				assertActive(t, handler01, true)
				assertConsumers(t, ts, queue, 1)

				// trigger cancelation
				err = amqpxPublish.Publish("exchange-03", "event-03", pool.Publishing{
					ContentType: "application/json",
					Body:        []byte(fmt.Sprintf("%s: delayed toggle back", eventContent)),
				})
				assert.NoError(t, err)
			}
		}
		return nil
	})

	var once sync.Once
	amqpx.RegisterBatchHandler("queue-03", func(msgs []pool.Delivery) (err error) {
		_ = msgs[0]
		once.Do(func() {
			queue := handler01.Queue()

			assertActive(t, handler01, true)
			err = handler01.Pause(context.Background())
			if err != nil {
				assert.NoError(t, err)
				return
			}
			assertActive(t, handler01, false)

			q1, err := ts.QueueDeclarePassive(queue)
			if err != nil {
				assert.NoError(t, err)
				return
			}

			// wait for potential further processing
			time.Sleep(3 * time.Second)

			q2, err := ts.QueueDeclarePassive(queue)
			if err != nil {
				assert.NoError(t, err)
				return
			}

			assert.Equal(t, q1, q2) // message count should also be equal
			assertConsumers(t, ts, queue, 0)

			go func() {
				// delay cancelation (due to ack)
				time.Sleep(3 * time.Second)
				cancel()
			}()
		})

		return nil
	})

	assertActive(t, handler01, false)

	err = amqpx.Start(
		connectURL,
		amqpx.WithLogger(log),
		amqpx.WithPublisherConnections(1),
		amqpx.WithPublisherSessions(5),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}

	// will be canceled when the event has reache dthe third handler
	<-ctx.Done()
	log.Info("context canceled, closing test.")
	err = amqpx.Close()
	assert.NoError(t, err)
}

func assertConsumers(t *testing.T, ts *pool.Session, queueName string, expected int) {
	// rabbitMQ needs some time before it updates its consumer count
	time.Sleep(time.Second)
	queue, err := ts.QueueDeclarePassive(queueName)
	if err != nil {
		assert.NoError(t, err)
		return
	}

	if expected != queue.Consumers {
		assert.Equal(t, expected, queue.Consumers, "consumer count of queue %s should be %d", queueName, expected)
		return
	}
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

type handlerStats interface {
	Queue() string
	IsActive(ctx context.Context) (active bool, err error)
}
