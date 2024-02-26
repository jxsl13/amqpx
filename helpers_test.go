package amqpx_test

import (
	"context"
	"errors"
	"fmt"

	"github.com/jxsl13/amqpx/pool"
)

func createTopology(ctx context.Context, t *pool.Topologer) (err error) {
	// documentation: https://www.cloudamqp.com/blog/part4-rabbitmq-for-beginners-exchanges-routing-keys-bindings.html#:~:text=The%20routing%20key%20is%20a%20message%20attribute%20added%20to%20the,routing%20key%20of%20the%20message.

	err = createExchange(ctx, "exchange-01", t)
	if err != nil {
		return err
	}

	err = createQueue(ctx, "queue-01", t)
	if err != nil {
		return err
	}

	err = t.QueueBind(ctx, "queue-01", "event-01", "exchange-01")
	if err != nil {
		return err
	}

	err = createExchange(ctx, "exchange-02", t)
	if err != nil {
		return err
	}

	err = createQueue(ctx, "queue-02", t)
	if err != nil {
		return err
	}

	err = t.QueueBind(ctx, "queue-02", "event-02", "exchange-02")
	if err != nil {
		return err
	}

	err = createExchange(ctx, "exchange-03", t)
	if err != nil {
		return err
	}

	err = createQueue(ctx, "queue-03", t)
	if err != nil {
		return err
	}
	err = t.QueueBind(ctx, "queue-03", "event-03", "exchange-03")
	if err != nil {
		return err
	}
	return nil

}

func deleteTopology(ctx context.Context, t *pool.Topologer) (err error) {
	err = deleteQueue(ctx, "queue-01", t)
	if err != nil {
		return err
	}

	err = deleteQueue(ctx, "queue-02", t)
	if err != nil {
		return err
	}

	err = deleteQueue(ctx, "queue-03", t)
	if err != nil {
		return err
	}

	err = deleteExchange(ctx, "exchange-01", t)
	if err != nil {
		return err
	}

	err = deleteExchange(ctx, "exchange-02", t)
	if err != nil {
		return err
	}

	err = deleteExchange(ctx, "exchange-03", t)
	if err != nil {
		return err
	}

	return nil
}

func createQueue(ctx context.Context, name string, t *pool.Topologer) (err error) {
	_, err = t.QueueDeclarePassive(ctx, name)
	if !errors.Is(err, pool.ErrNotFound) {
		if err != nil {
			return fmt.Errorf("queue %s was found even tho it should not exist: %w", name, err)
		}
		return fmt.Errorf("queue %s was found even tho it should not exist", name)
	}

	_, err = t.QueueDeclare(ctx, name)
	if err != nil {
		return err
	}

	_, err = t.QueueDeclarePassive(ctx, name)
	if err != nil {
		return fmt.Errorf("queue %s was not found even tho it should exist: %w", name, err)
	}
	return nil
}

func deleteQueue(ctx context.Context, name string, t *pool.Topologer) (err error) {
	_, err = t.QueueDeclarePassive(ctx, name)
	if err != nil {
		return fmt.Errorf("%q does not exist but is supposed to be deleted", name)
	}

	_, err = t.QueueDelete(ctx, name)
	if err != nil {
		return err
	}

	_, err = t.QueueDeclarePassive(ctx, name)
	if err == nil {
		return fmt.Errorf("%q still exists after deletion", name)
	}
	return nil
}

func createExchange(ctx context.Context, name string, t *pool.Topologer) (err error) {
	err = t.ExchangeDeclarePassive(ctx, name, pool.ExchangeKindTopic)
	if !errors.Is(err, pool.ErrNotFound) {
		if err != nil {
			return fmt.Errorf("exchange %s was found even tho it should not exist: %w", name, err)
		}
		return fmt.Errorf("exchange %s was found even tho it should not exist", name)
	}

	err = t.ExchangeDeclare(ctx, name, pool.ExchangeKindTopic)
	if err != nil {
		return err
	}

	err = t.ExchangeDeclarePassive(ctx, name, pool.ExchangeKindTopic)
	if err != nil {
		return fmt.Errorf("exchange %s was not found even tho it should exist: %w", name, err)
	}
	return nil
}

func deleteExchange(ctx context.Context, name string, t *pool.Topologer) (err error) {
	err = t.ExchangeDeclarePassive(ctx, name, pool.ExchangeKindTopic)
	if err != nil {
		return fmt.Errorf("exchange %s was not found even tho it should exist: %w", name, err)
	}

	err = t.ExchangeDelete(ctx, name)
	if err != nil {
		return err
	}

	err = t.ExchangeDeclarePassive(ctx, name, pool.ExchangeKindTopic)
	if !errors.Is(err, pool.ErrNotFound) {
		return fmt.Errorf("exchange %s was found even tho it should not exist: %w", name, err)
	}
	return nil
}
