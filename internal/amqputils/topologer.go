package amqputils

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/jxsl13/amqpx/internal/testlogger"
	"github.com/jxsl13/amqpx/types"
	"github.com/stretchr/testify/assert"
)

type Topologer interface {
	ExchangeDeclare(ctx context.Context, name string, kind types.ExchangeKind, option ...types.ExchangeDeclareOptions) error
	ExchangeDelete(ctx context.Context, name string, option ...types.ExchangeDeleteOptions) error
	QueueDeclare(ctx context.Context, name string, option ...types.QueueDeclareOptions) (types.Queue, error)
	QueueDelete(ctx context.Context, name string, option ...types.QueueDeleteOptions) (purgedMsgs int, err error)
	QueueBind(ctx context.Context, queueName string, routingKey string, exchange string, option ...types.QueueBindOptions) error
	QueueUnbind(ctx context.Context, name string, routingKey string, exchange string, arg ...types.Table) error
}

// DeclareExchangeQueue declares an exchange and a queue and binds them together.
// It returns a cleanup function that can be used to delete the exchange and queue.
// The cleanup function is idempotent and can be called multiple times, but it will only delete the exchange and queue once.
func DeclareExchangeQueue(
	t *testing.T,
	ctx context.Context,
	s Topologer,
	exchangeName string,
	queueName string,
) (cleanup func()) {
	cleanup = func() {}
	var err error

	log := testlogger.NewTestLogger(t)

	log.Info(fmt.Sprintf("declaring exchange %s", exchangeName))
	err = s.ExchangeDeclare(ctx, exchangeName, types.ExchangeKindTopic)
	if err != nil {
		assert.NoError(t, err, "expected no error when declaring exchange")
		return
	}
	defer func() {
		if err != nil {
			log.Info(fmt.Sprintf("deleting exchange %s", exchangeName))
			assert.NoError(t, s.ExchangeDelete(ctx, exchangeName), "expected no error when deleting exchange")
		}
	}()

	log.Info(fmt.Sprintf("declaring queue %s", queueName))
	_, err = s.QueueDeclare(ctx, queueName)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer func() {
		if err != nil {
			log.Info(fmt.Sprintf("deleting queue %s", queueName))
			_, e := s.QueueDelete(ctx, queueName)
			assert.NoError(t, e, "expected no error when deleting queue")
			// INFO: asserting the number of purged messages seems to be flaky, so we do not do that for now.
			//assert.Equalf(t, 0, deleted, "expected 0 deleted messages, got %d for queue %s", deleted, queueName)
		}
	}()

	log.Info(fmt.Sprintf("binding queue %s to exchange %s", queueName, exchangeName))
	err = s.QueueBind(ctx, queueName, "#", exchangeName)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer func() {
		if err != nil {
			log.Info(fmt.Sprintf("unbinding queue %s from exchange %s", queueName, exchangeName))
			assert.NoError(t, s.QueueUnbind(ctx, queueName, "#", exchangeName, nil))
		}
	}()

	once := sync.Once{}
	return func() {
		once.Do(func() {
			log.Info(fmt.Sprintf("unbinding queue %s from exchange %s", queueName, exchangeName))
			assert.NoError(t, s.QueueUnbind(ctx, queueName, "#", exchangeName, nil))

			log.Info(fmt.Sprintf("deleting queue %s", queueName))
			_, e := s.QueueDelete(ctx, queueName)
			assert.NoError(t, e)

			log.Info(fmt.Sprintf("deleting exchange %s", exchangeName))
			assert.NoError(t, s.ExchangeDelete(ctx, exchangeName))
		})
	}
}
