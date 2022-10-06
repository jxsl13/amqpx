package amqpx_test

import (
	"context"
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

func TestAMQPX(t *testing.T) {
	log := logging.NewTestLogger(t)

	amqpx.RegisterTopology(func(t *amqpx.Topologer) error {

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

		err = t.QueueDeclare("queue-03", true, false, false, false, amqpx.QuorumQueue)
		if err != nil {
			return err
		}

		err = t.QueueBind("queue-03", "event-03", "exchange-01", false, nil)
		if err != nil {
			return err
		}
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	eventContent := "event content"

	amqpx.RegisterHandler("queue-01", "", false, false, false, false, nil, func(msg amqpx.Delivery) error {
		return amqpx.Publish("exchange-01", "event-02", false, false, amqpx.Publishing{
			ContentType: "application/json",
			Body:        []byte(eventContent),
		})
	})

	amqpx.RegisterHandler("queue-02", "", false, false, false, false, nil, func(msg amqpx.Delivery) error {
		return amqpx.Publish("exchange-01", "event-03", false, false, amqpx.Publishing{
			ContentType: "application/json",
			Body:        []byte(eventContent),
		})
	})

	amqpx.RegisterHandler("queue-03", "", false, false, false, false, nil, func(msg amqpx.Delivery) error {
		cancel()
		return nil
	})

	time.Sleep(5 * time.Second)
	err := amqpx.Start(
		amqpx.NewURL("localhost", 5672, "admin", "password"),
		amqpx.WithLogger(log),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer amqpx.Close()

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
}
