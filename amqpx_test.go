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

// WARNING: Do not assert consumer counts, as those values are too flaky and break tests all over th eplace
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(
		m,
		goleak.IgnoreTopFunction("internal/poll.runtime_pollWait"),
		goleak.IgnoreTopFunction("github.com/rabbitmq/amqp091-go.(*Connection).heartbeater"),
		goleak.IgnoreTopFunction("net/http.(*persistConn).writeLoop"),
	)
}

func TestExchangeDeclarePassive(t *testing.T) {
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
		amqpx.WithLogger(logging.NewTestLogger(t)),
		amqpx.WithPublisherConnections(1),
		amqpx.WithPublisherSessions(2),
	)
	assert.NoError(t, err)
}

func TestQueueDeclarePassive(t *testing.T) {
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
		amqpx.WithLogger(logging.NewTestLogger(t)),
		amqpx.WithPublisherConnections(1),
		amqpx.WithPublisherSessions(2),
	)
	assert.NoError(t, err)
}

func TestAMQPXPub(t *testing.T) {
	defer amqpx.Reset()

	amqpx.RegisterTopologyCreator(createTopology)
	amqpx.RegisterTopologyDeleter(deleteTopology)

	err := amqpx.Start(
		connectURL,
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
		amqpx.WithLogger(logging.NewNoOpLogger()),
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
			assert.NoError(t, err)
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
		amqpx.WithLogger(logging.NewNoOpLogger()),
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
	var err error
	queueName := "testPauseResumeHandler-01"
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGINT)
	defer cancel()

	log := logging.NewTestLogger(t)

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

	err = amqp.Start(connectURL,
		amqpx.WithLogger(
			logging.NewNoOpLogger(),
		),
		amqpx.WithContext(ctx),
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

		err = handler.Pause(context.Background())
		if err != nil {
			assert.NoError(t, err)
			return
		}

		assertActive(t, handler, false)

		err = handler.Resume(context.Background())
		if err != nil {
			assert.NoError(t, err)
			return
		}

		assertActive(t, handler, true)
	}
}

func TestHandlerPauseAndResume(t *testing.T) {
	for i := 0; i < 5; i++ {
		t.Logf("iteration %d", i)
		testHandlerPauseAndResume(t)
	}
}

func testHandlerPauseAndResume(t *testing.T) {
	var err error
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGINT)
	defer cancel()

	log := logging.NewTestLogger(t)
	defer func() {
		assert.NoError(t, amqpx.Reset())
	}()

	options := []amqpx.Option{
		amqpx.WithLogger(logging.NewNoOpLogger()),
		amqpx.WithPublisherConnections(1),
		amqpx.WithPublisherSessions(5),
	}

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
			err = amqpxPublish.Publish("exchange-03", "event-03", pool.Publishing{
				ContentType: "application/json",
				Body:        []byte(fmt.Sprintf("%s: delayed toggle back", eventContent)),
			})
			assert.NoError(t, err)
		}
		return nil
	})

	amqpx.RegisterHandler("queue-03", func(msg pool.Delivery) (err error) {

		assertActive(t, handler01, true)
		err = handler01.Pause(context.Background())
		if err != nil {
			assert.NoError(t, err)
			return nil
		}
		assertActive(t, handler01, false)

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
		amqpx.WithLogger(logging.NewNoOpLogger()),
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
	assert.NoError(t, amqpx.Close())
	assertActive(t, handler01, false)
}

func TestBatchHandlerPauseAndResume(t *testing.T) {
	for i := 0; i < 5; i++ {
		testBatchHandlerPauseAndResume(t)
	}
}

func testBatchHandlerPauseAndResume(t *testing.T) {
	var err error
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGINT)
	defer cancel()

	log := logging.NewTestLogger(t)
	defer func() {
		assert.NoError(t, amqpx.Reset())
	}()

	options := []amqpx.Option{
		amqpx.WithLogger(logging.NewNoOpLogger()),
		amqpx.WithPublisherConnections(1),
		amqpx.WithPublisherSessions(5),
	}

	amqpxPublish := amqpx.New()
	amqpxPublish.RegisterTopologyCreator(createTopology)
	err = amqpxPublish.Start(connectURL, options...)
	require.NoError(t, err)

	eventContent := "TestBatchHandlerPauseAndResume - event content"

	var (
		publish = 5000
		cnt     = 0
	)

	// step 1 - fill queue with messages
	amqpx.RegisterTopologyDeleter(deleteTopology)

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

			assertActive(t, handler01, true)
			err = handler01.Pause(context.Background())
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

	err = amqpx.Start(
		connectURL,
		amqpx.WithLogger(logging.NewNoOpLogger()),
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
	assert.NoError(t, amqpx.Close())
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

type handlerStats interface {
	Queue() string
	IsActive(ctx context.Context) (active bool, err error)
}

func TestHandlerReset(t *testing.T) {
	for i := 0; i < 5; i++ {
		testHandlerReset(t)
	}
	t.Log("done")
}

func testHandlerReset(t *testing.T) {
	var err error
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGINT)
	defer cancel()

	log := logging.NewTestLogger(t)
	defer func() {
		assert.NoError(t, amqpx.Reset())
	}()

	options := []amqpx.Option{
		amqpx.WithLogger(logging.NewNoOpLogger()),
		amqpx.WithPublisherConnections(1),
		amqpx.WithPublisherSessions(5),
	}

	amqpxPublish := amqpx.New()
	amqpxPublish.RegisterTopologyCreator(createTopology)
	err = amqpxPublish.Start(connectURL, options...)
	require.NoError(t, err)

	eventContent := "TestBatchHandlerReset - event content"

	var (
		publish = 50
		cnt     = 0
	)

	// step 1 - fill queue with messages
	amqpx.RegisterTopologyDeleter(deleteTopology)

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

	done := make(chan struct{})
	// step 2 - process messages, pause, wait, resume, process rest, cancel context
	handler01 := amqpx.RegisterHandler("queue-01", func(msgs pool.Delivery) (err error) {
		cnt++
		if cnt == publish {
			close(done)
		}
		return nil
	})

	assertActive(t, handler01, false)

	err = amqpx.Start(
		connectURL,
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

	<-ctx.Done()
	log.Info("context canceled, closing test.")
	assert.NoError(t, amqpx.Close())

	// after close
	assertActive(t, handler01, false)
}

func TestBatchHandlerReset(t *testing.T) {
	for i := 0; i < 5; i++ {
		testBatchHandlerReset(t)
	}
	t.Log("done")
}

func testBatchHandlerReset(t *testing.T) {
	var err error
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGINT)
	defer cancel()

	log := logging.NewTestLogger(t)
	defer func() {
		assert.NoError(t, amqpx.Reset())
	}()

	options := []amqpx.Option{
		amqpx.WithLogger(logging.NewNoOpLogger()),
		amqpx.WithPublisherConnections(1),
		amqpx.WithPublisherSessions(5),
	}

	amqpxPublish := amqpx.New()
	amqpxPublish.RegisterTopologyCreator(createTopology)
	err = amqpxPublish.Start(connectURL, options...)
	require.NoError(t, err)

	eventContent := "TestBatchHandlerReset - event content"

	var (
		publish = 50
		cnt     = 0
	)

	// step 1 - fill queue with messages
	amqpx.RegisterTopologyDeleter(deleteTopology)

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

	done := make(chan struct{})
	// step 2 - process messages, pause, wait, resume, process rest, cancel context
	handler01 := amqpx.RegisterBatchHandler("queue-01", func(msgs []pool.Delivery) (err error) {
		cnt += len(msgs)

		if cnt == publish {
			close(done)
		}
		return nil
	})

	assertActive(t, handler01, false)

	err = amqpx.Start(
		connectURL,
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

	<-ctx.Done()
	log.Info("context canceled, closing test.")
	assert.NoError(t, amqpx.Close())

	// after close
	assertActive(t, handler01, false)
}
