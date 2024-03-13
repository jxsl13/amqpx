package amqpx_test

import (
	"context"
	"errors"
	"fmt"

	"github.com/jxsl13/amqpx"
	"github.com/jxsl13/amqpx/pool"
)

func createTopology(prefix string) amqpx.TopologyFunc {
	return func(ctx context.Context, t *pool.Topologer) (err error) {
		// documentation: https://www.cloudamqp.com/blog/part4-rabbitmq-for-beginners-exchanges-routing-keys-bindings.html#:~:text=The%20routing%20key%20is%20a%20message%20attribute%20added%20to%20the,routing%20key%20of%20the%20message.

		err = createExchange(ctx, prefix+"exchange-01", t)
		if err != nil {
			return err
		}

		err = createQueue(ctx, prefix+"queue-01", t)
		if err != nil {
			return err
		}

		err = t.QueueBind(ctx, prefix+"queue-01", "event-01", prefix+"exchange-01")
		if err != nil {
			return err
		}

		err = createExchange(ctx, prefix+"exchange-02", t)
		if err != nil {
			return err
		}

		err = createQueue(ctx, prefix+"queue-02", t)
		if err != nil {
			return err
		}

		err = t.QueueBind(ctx, prefix+"queue-02", "event-02", prefix+"exchange-02")
		if err != nil {
			return err
		}

		err = createExchange(ctx, prefix+"exchange-03", t)
		if err != nil {
			return err
		}

		err = createQueue(ctx, prefix+"queue-03", t)
		if err != nil {
			return err
		}
		err = t.QueueBind(ctx, prefix+"queue-03", "event-03", prefix+"exchange-03")
		if err != nil {
			return err
		}
		return nil
	}
}

func deleteTopology(prefix string) amqpx.TopologyFunc {
	return func(ctx context.Context, t *pool.Topologer) (err error) {
		err = deleteQueue(ctx, prefix+"queue-01", t)
		if err != nil {
			return err
		}

		err = deleteQueue(ctx, prefix+"queue-02", t)
		if err != nil {
			return err
		}

		err = deleteQueue(ctx, prefix+"queue-03", t)
		if err != nil {
			return err
		}

		err = deleteExchange(ctx, prefix+"exchange-01", t)
		if err != nil {
			return err
		}

		err = deleteExchange(ctx, prefix+"exchange-02", t)
		if err != nil {
			return err
		}

		err = deleteExchange(ctx, prefix+"exchange-03", t)
		if err != nil {
			return err
		}

		return nil
	}
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
		return fmt.Errorf("%q does not exist but is supposed to be deleted: %w", name, err)
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
