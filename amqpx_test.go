package amqpx_test

import (
	"context"
	"fmt"
	"os/signal"
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
	goleak.VerifyTestMain(m)
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
		amqpx.WithPoolOption(pool.WithSlowClose(true)), // needed for goroutine leak tests
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
		amqpx.WithPoolOption(pool.WithSlowClose(true)), // needed for goroutine leak tests
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
		amqpx.WithPoolOption(pool.WithSlowClose(true)), // needed for goroutine leak tests
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

	amqpx.RegisterHandler(ctx, "queue-01", func(msg pool.Delivery) error {
		log.Info("subscriber of queue-01")
		cancel()
		return nil
	})

	err := amqpx.Start(
		connectURL,
		amqpx.WithLogger(log),
		amqpx.WithPublisherConnections(1),
		amqpx.WithPublisherSessions(5),
		amqpx.WithPoolOption(pool.WithSlowClose(true)), // needed for goroutine leaks tests
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
	amqpx.RegisterHandler(ctx, "queue-01", func(msg pool.Delivery) error {
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

	amqpx.RegisterHandler(ctx, "queue-02", func(msg pool.Delivery) error {
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

	amqpx.RegisterHandler(ctx, "queue-03", func(msg pool.Delivery) error {
		log.Info("handler of subscriber-03: canceling context!")
		cancel()
		return nil
	}, pool.ConsumeOptions{ConsumerTag: "subscriber-03"})

	err := amqpx.Start(
		connectURL,
		amqpx.WithLogger(log),
		amqpx.WithPublisherConnections(1),
		amqpx.WithPublisherSessions(5),
		amqpx.WithPoolOption(pool.WithSlowClose(true)), // needed for goroutine leak tests
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

	amqpx.RegisterHandler(ctx, "queue-01", func(msg pool.Delivery) error {
		log.Info("subscriber of queue-01")
		cancel()
		return nil
	})

	err := amqpx.Start(
		connectURL,
		amqpx.WithLogger(log),
		amqpx.WithPublisherConnections(1),
		amqpx.WithPublisherSessions(5),
		amqpx.WithPoolOption(pool.WithSlowClose(true)), // needed for goroutine leaks tests
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
		amqpx.WithPoolOption(pool.WithSlowClose(true)), // needed for goroutine leak tests
	)
	assert.NoError(t, err)
}

func TestHandlerPauseAndResume(t *testing.T) {
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
		pool.WithSlowClose(true), // needed for goroutine leaks tests
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
	processingHandler := amqpx.RegisterHandler(ctx, "queue-01", func(msg pool.Delivery) (err error) {
		cnt++
		if cnt == publish/2 {
			err = amqpx.Publish("exchange-02", "event-02", pool.Publishing{
				ContentType: "application/json",
				Body:        []byte(fmt.Sprintf("%s: hit %d messages, toggling processing", eventContent, cnt)),
			})
			require.NoError(t, err)
		}

		if cnt == publish {
			cancel()
		}

		return nil
	})

	running := true
	amqpx.RegisterHandler(ctx, "queue-02", func(msg pool.Delivery) (err error) {
		log.Infof("received toggle request: %s", string(msg.Body))
		if running {
			p1, err := ts.QueueDeclarePassive("queue-01")
			assert.NoError(t, err)
			if err != nil {
				return nil
			}
			assert.Equal(t, 1, p1.Consumers, "should have one consumer at p1 before pausing")

			err = processingHandler.Pause(ctx)
			require.NoError(t, err)
			log.Infof("paused processing of %s", processingHandler.Queue())
			running = false

			p2, err := ts.QueueDeclarePassive("queue-01")
			assert.NoError(t, err)
			if err != nil {
				return nil
			}
			assert.Equal(t, 0, p2.Consumers, "should have no consumers at p2 after pausing")

			amqpx.Publish("exchange-03", "event-03", pool.Publishing{
				ContentType: "application/json",
				Body:        []byte(fmt.Sprintf("%s: delayed toggle back", eventContent)),
			})

		} else {

			r1, err := ts.QueueDeclarePassive("queue-01")
			assert.NoError(t, err)
			if err != nil {
				return nil
			}
			assert.Equal(t, 0, r1.Consumers, "should have no consumers at r1 before resuming")

			err = processingHandler.Resume(ctx)
			require.NoError(t, err)
			log.Infof("resumed processing of %s", processingHandler.Queue())
			running = true

			r2, err := ts.QueueDeclarePassive("queue-01")
			assert.NoError(t, err)
			if err != nil {
				return nil
			}
			assert.Equal(t, 1, r2.Consumers, "should have 1 consumer at r2 after resuming")
		}
		return nil
	})

	amqpx.RegisterHandler(ctx, "queue-03", func(msg pool.Delivery) (err error) {
		defer func() {
			// always resume processing
			err = amqpx.Publish("exchange-02", "event-02", pool.Publishing{
				ContentType: "application/json",
				Body:        []byte(fmt.Sprintf("%s: delayed toggle", eventContent)),
			})
			require.NoError(t, err)
		}()

		q1, err := ts.QueueDeclarePassive("queue-01")
		assert.NoError(t, err)
		if err != nil {
			return nil
		}

		// wait for potential further processing
		time.Sleep(3 * time.Second)

		q2, err := ts.QueueDeclarePassive("queue-01")
		assert.NoError(t, err)
		if err != nil {
			return nil
		}

		assert.Equal(t, q1, q2)
		assert.Equal(t, 0, q1.Consumers, "should have no consumers at q1")
		assert.Equal(t, 0, q2.Consumers, "should have no consumers at q2")

		return nil
	})

	err = amqpx.Start(
		connectURL,
		amqpx.WithLogger(log),
		amqpx.WithPublisherConnections(1),
		amqpx.WithPublisherSessions(5),
		amqpx.WithPoolOption(pool.WithSlowClose(true)), // needed for goroutine leaks tests
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
