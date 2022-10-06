package amqpx_test

import (
	"context"
	"os/signal"
	"syscall"
	"testing"

	"github.com/jxsl13/amqpx"
	"github.com/jxsl13/amqpx/logging"
	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func createTopology(t *amqpx.Topologer) error {
	err := t.ExchangeDeclare("exchange-01", "fanout", true, false, false, false, nil)
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

	err = t.QueueDeclare("queue-02", true, false, false, false, amqpx.QuorumQueue)
	if err != nil {
		return err
	}

	err = t.QueueBind("queue-02", "event-02", "exchange-01", false, nil)
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

	err = t.ExchangeDelete("exchange-01", false, false)
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
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}

	// publish event to first queue
	err = amqpx.Publish("exchange-01", "event-01", false, false, amqpx.Publishing{
		ContentType: "application/json",
		Body:        []byte("TestAMQPXPub - event content"),
	})
	if err != nil {
		assert.NoError(t, err)
		return
	}

	// will be canceled when the event has reache dthe third handler
	err = amqpx.Close()
	assert.NoError(t, err)
}

func TestAMQPXSubAndPub(t *testing.T) {
	log := logging.NewTestLogger(t)
	defer amqpx.Reset()

	amqpx.RegisterTopologyCreator(createTopology)
	amqpx.RegisterTopologyDeleter(deleteTopology)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGINT)
	defer cancel()

	// eventContent := "TestAMQPXSubAndPub - event content"

	amqpx.RegisterHandler("queue-01", "", false, false, false, false, nil, func(msg amqpx.Delivery) error {
		log.Info("subscriber of queue-01")
		cancel()
		return nil
	})

	err := amqpx.Start(
		amqpx.NewURL("localhost", 5672, "admin", "password"),
		amqpx.WithLogger(log),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}

	// publish event to first queue
	/*
		err = amqpx.Publish("exchange-01", "event-01", false, false, amqpx.Publishing{
			ContentType: "application/json",
			Body:        []byte(eventContent),
		})
		if err != nil {
			assert.NoError(t, err)
			return
		}
	*/
	// will be canceled when the event has reache dthe third handler
	<-ctx.Done()
	log.Info("context canceled, closing test.")
	err = amqpx.Close()
	assert.NoError(t, err)
}
