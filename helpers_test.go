package amqpx_test

import (
	"context"
	"errors"
	"fmt"

	"github.com/jxsl13/amqpx"
	"github.com/jxsl13/amqpx/internal/testutils"
	"github.com/jxsl13/amqpx/logging"
	"github.com/jxsl13/amqpx/pool"
)

func declareExchangeQueue(ctx context.Context, eq testutils.ExchangeQueue, t *pool.Topologer, log logging.Logger) (err error) {
	err = createExchange(ctx, eq.Exchange, t, log)
	if err != nil {
		return err
	}

	err = createQueue(ctx, eq.Queue, t, log)
	if err != nil {
		return err
	}

	err = bindQueue(ctx, eq.Queue, eq.Exchange, eq.RoutingKey, t, log)
	if err != nil {
		return err
	}
	return nil
}

func deleteExchangeQueue(ctx context.Context, eq testutils.ExchangeQueue, t *pool.Topologer, log logging.Logger) (err error) {
	err = unbindQueue(ctx, eq.Queue, eq.Exchange, eq.RoutingKey, t, log)
	if err != nil {
		return err
	}

	err = deleteQueue(ctx, eq.Queue, t, log)
	if err != nil {
		return err
	}

	err = deleteExchange(ctx, eq.Exchange, t, log)
	if err != nil {
		return err
	}
	return nil
}

func createTopology(log logging.Logger, eqs ...testutils.ExchangeQueue) amqpx.TopologyFunc {
	return func(ctx context.Context, t *pool.Topologer) (err error) {
		// documentation: https://www.cloudamqp.com/blog/part4-rabbitmq-for-beginners-exchanges-routing-keys-bindings.html#:~:text=The%20routing%20key%20is%20a%20message%20attribute%20added%20to%20the,routing%20key%20of%20the%20message.

		for _, eq := range eqs {
			err = declareExchangeQueue(ctx, eq, t, log)
			if err != nil {
				return err
			}
		}
		return nil
	}
}

func deleteTopology(log logging.Logger, eqs ...testutils.ExchangeQueue) amqpx.TopologyFunc {
	return func(ctx context.Context, t *pool.Topologer) (err error) {

		for _, eq := range eqs {
			err = deleteExchangeQueue(ctx, eq, t, log)
			if err != nil {
				return err
			}
		}
		return nil
	}
}

func createQueue(ctx context.Context, name string, t *pool.Topologer, log logging.Logger) (err error) {
	log.Infof("topology: creating queue: %s", name)

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

func deleteQueue(ctx context.Context, name string, t *pool.Topologer, log logging.Logger) (err error) {
	log.Infof("topology: deleting queue: %s", name)

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

func createExchange(ctx context.Context, name string, t *pool.Topologer, log logging.Logger) (err error) {
	log.Infof("topology: creating exchange: %s", name)

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

func deleteExchange(ctx context.Context, name string, t *pool.Topologer, log logging.Logger) (err error) {
	log.Infof("topology: deleting exchange: %s", name)

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

func bindQueue(ctx context.Context, queue, exchange, routingKey string, t *pool.Topologer, log logging.Logger) (err error) {
	log.Infof("topology: binding queue %s to exchange %s with routing key: %s", queue, exchange, routingKey)
	err = t.QueueBind(ctx, queue, routingKey, exchange)
	if err != nil {
		return err
	}
	return nil
}

func unbindQueue(ctx context.Context, queue, exchange, routingKey string, t *pool.Topologer, log logging.Logger) (err error) {
	log.Infof("topology: unbinding queue %s from exchange %s with routing key: %s", queue, exchange, routingKey)
	err = t.QueueUnbind(ctx, queue, routingKey, exchange)
	if err != nil {
		return err
	}
	return nil
}
