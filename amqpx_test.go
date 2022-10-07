package amqpx_test

import (
	"context"
	"os/signal"
	"syscall"
	"testing"
	"time"

	"github.com/jxsl13/amqpx"
	"github.com/jxsl13/amqpx/logging"
	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func createTopology(t *amqpx.Topologer) error {
	// documentation: https://www.cloudamqp.com/blog/part4-rabbitmq-for-beginners-exchanges-routing-keys-bindings.html#:~:text=The%20routing%20key%20is%20a%20message%20attribute%20added%20to%20the,routing%20key%20of%20the%20message.
	err := t.ExchangeDeclare("exchange-01", "topic", true, false, false, false, nil)
	if err != nil {
		return err
	}

	err = t.QueueDeclare("queue-01", true, false, false, false, amqpx.QuorumQueue)
	if err != nil {
		return err
	}

	err = t.QueueBind("queue-01", "event-01", "exchange-01", false, nil)
	if err != nil {
		return err
	}

	err = t.ExchangeDeclare("exchange-02", "topic", true, false, false, false, nil)
	if err != nil {
		return err
	}

	err = t.QueueDeclare("queue-02", true, false, false, false, amqpx.QuorumQueue)
	if err != nil {
		return err
	}

	err = t.QueueBind("queue-02", "event-02", "exchange-02", false, nil)
	if err != nil {
		return err
	}

	err = t.ExchangeDeclare("exchange-03", "topic", true, false, false, false, nil)
	if err != nil {
		return err
	}

	err = t.QueueDeclare("queue-03", true, false, false, false, amqpx.QuorumQueue)
	if err != nil {
		return err
	}

	err = t.QueueBind("queue-03", "event-03", "exchange-03", false, nil)
	if err != nil {
		return err
	}
	return nil
}

func deleteTopology(t *amqpx.Topologer) error {
	_, err := t.QueueDelete("queue-01", false, false, false)
	if err != nil {
		return err
	}

	_, err = t.QueueDelete("queue-02", false, false, false)
	if err != nil {
		return err
	}

	_, err = t.QueueDelete("queue-03", false, false, false)
	if err != nil {
		return err
	}

	err = t.ExchangeDelete("exchange-01", false, false)
	if err != nil {
		return err
	}

	err = t.ExchangeDelete("exchange-02", false, false)
	if err != nil {
		return err
	}

	err = t.ExchangeDelete("exchange-03", false, false)
	if err != nil {
		return err
	}

	return nil
}

func TestAMQPXPub(t *testing.T) {
	log := logging.NewTestLogger(t)
	defer amqpx.Reset()

	amqpx.RegisterTopologyCreator(createTopology)
	amqpx.RegisterTopologyDeleter(deleteTopology)

	err := amqpx.Start(
		amqpx.NewURL("localhost", 5672, "admin", "password"),
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
	err = amqpx.Publish("exchange-01", "event-01", false, false, amqpx.Publishing{
		ContentType: "application/json",
		Body:        []byte(event),
	})
	if err != nil {
		assert.NoError(t, err)
		return
	}

	var (
		msg *amqpx.Delivery
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

	if msg == nil || !ok {
		assert.NotNil(t, msg)
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

	amqpx.RegisterHandler("queue-01", "", false, false, false, false, nil, func(msg amqpx.Delivery) error {
		log.Info("subscriber of queue-01")
		cancel()
		return nil
	})

	err := amqpx.Start(
		amqpx.NewURL("localhost", 5672, "admin", "password"),
		amqpx.WithLogger(log),
		amqpx.WithPublisherConnections(1),
		amqpx.WithPublisherSessions(5),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}

	// publish event to first queue

	err = amqpx.Publish("exchange-01", "event-01", false, false, amqpx.Publishing{
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
	amqpx.RegisterHandler("queue-01", "subscriber-01", false, false, false, false, nil, func(msg amqpx.Delivery) error {
		log.Info("handler of subscriber-01")

		err := amqpx.Publish("exchange-02", "event-02", false, false, amqpx.Publishing{
			ContentType: msg.ContentType,
			Body:        msg.Body,
		})

		if err != nil {
			log.Error("subscriber-01:", err)
		}

		return nil
	})

	amqpx.RegisterHandler("queue-02", "subscriber-02", false, false, false, false, nil, func(msg amqpx.Delivery) error {
		log.Info("handler of subscriber-02")

		err := amqpx.Publish("exchange-03", "event-03", false, false, amqpx.Publishing{
			ContentType: msg.ContentType,
			Body:        msg.Body,
		})

		if err != nil {
			log.Error("subscriber-02:", err)
		}

		return nil
	})

	amqpx.RegisterHandler("queue-03", "subscriber-03", false, false, false, false, nil, func(msg amqpx.Delivery) error {
		log.Info("handler of subscriber-03: canceling context!")
		cancel()
		return nil
	})

	err := amqpx.Start(
		amqpx.NewURL("localhost", 5672, "admin", "password"),
		amqpx.WithLogger(log),
		amqpx.WithPublisherConnections(1),
		amqpx.WithPublisherSessions(5),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}

	// publish event to first queue

	err = amqpx.Publish("exchange-01", "event-01", false, false, amqpx.Publishing{
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
