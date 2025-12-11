package pool_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jxsl13/amqpx/internal/amqputils"
	"github.com/jxsl13/amqpx/internal/proxyutils"
	"github.com/jxsl13/amqpx/internal/testlogger"
	"github.com/jxsl13/amqpx/internal/testutils"
	"github.com/jxsl13/amqpx/pool"
	"github.com/jxsl13/amqpx/types"
	"github.com/stretchr/testify/assert"
)

func TestSinglePublisher(t *testing.T) {
	t.Parallel()

	var (
		proxyName, connectURL, _ = testutils.NextConnectURL()
		ctx                      = context.TODO()
		log                      = testlogger.NewTestLogger(t)
		nextConnName             = testutils.ConnectionNameGenerator()
		numMsgs                  = 5
	)

	hs, hsclose := amqputils.NewSession(
		t,
		ctx,
		testutils.HealthyConnectURL,
		nextConnName(),
	)
	defer hsclose()

	p, err := pool.New(
		ctx,
		connectURL,
		1,
		1,
		pool.WithLogger(testlogger.NewTestLogger(t)),
		pool.WithConfirms(true),
		pool.WithConnectionRecoverCallback(func(name string, retry int, err error) {
			log.Warn(fmt.Sprintf("connection %s is broken, retry %d, error: %s", name, retry, err))
		}),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer p.Close()

	var (
		nextExchangeName = testutils.ExchangeNameGenerator(hs.Name())
		nextQueueName    = testutils.QueueNameGenerator(hs.Name())
		exchangeName     = nextExchangeName()
		queueName        = nextQueueName()
	)
	cleanup := amqputils.DeclareExchangeQueue(t, ctx, hs, exchangeName, queueName)
	defer cleanup()

	var (
		nextConsumerName = testutils.ConsumerNameGenerator(queueName)
		publisherMsgGen  = testutils.MessageGenerator(queueName)
		consumerMsgGen   = testutils.MessageGenerator(queueName)
		wg               sync.WaitGroup
	)

	pub := pool.NewPublisher(p)
	defer pub.Close()

	// INFO: currently this test allows duplication of messages
	amqputils.ConsumeAsyncN(t, ctx, &wg, hs, queueName, nextConsumerName(), consumerMsgGen, numMsgs, true)

	for i := 0; i < numMsgs; i++ {
		msg := publisherMsgGen()
		err = func(msg string) error {
			disconnected, reconnected := proxyutils.DisconnectWithStartedStopped(
				t,
				proxyName,
				0,
				testutils.Jitter(time.Millisecond, 20*time.Millisecond),
				testutils.Jitter(100*time.Millisecond, 150*time.Millisecond),
			)
			defer func() {
				disconnected()
				reconnected()
			}()

			return pub.Publish(ctx, exchangeName, "", types.Publishing{
				Mandatory:   true,
				ContentType: "text/plain",
				Body:        []byte(msg),
			})
		}(msg)
		if err != nil {
			assert.NoError(t, err, fmt.Sprintf("when publishing message %s", msg))
			return
		}
	}
	wg.Wait()
}

func TestPublisherPublishBatch(t *testing.T) {
	t.Parallel()

	var (
		proxyName, connectURL, _ = testutils.NextConnectURL()
		ctx, cancel              = context.WithCancel(t.Context())
		log                      = testlogger.NewTestLogger(t)
		nextConnName             = testutils.ConnectionNameGenerator()
		batchSize                = 100
		numBatches               = 10
	)
	defer cancel()

	hs, hsclose := amqputils.NewSession(
		t,
		ctx,
		testutils.HealthyConnectURL,
		nextConnName(),
	)
	defer hsclose()

	p, err := pool.New(
		ctx,
		connectURL,
		1,
		1,
		pool.WithLogger(testlogger.NewTestLogger(t)),
		pool.WithConfirms(true),
		pool.WithConnectionRecoverCallback(func(name string, retry int, err error) {
			log.Warn(fmt.Sprintf("connection %s is broken, retry %d, error: %s", name, retry, err))
		}),
	)
	if !assert.NoError(t, err) {
		return
	}
	defer p.Close()

	var (
		nextExchangeName = testutils.ExchangeNameGenerator(hs.Name())
		nextQueueName    = testutils.QueueNameGenerator(hs.Name())
		exchangeName     = nextExchangeName()
		queueName        = nextQueueName()
	)
	cleanup := amqputils.DeclareExchangeQueue(t, ctx, hs, exchangeName, queueName)
	defer cleanup()

	var (
		nextConsumerName = testutils.ConsumerNameGenerator(queueName)
		publisherMsgGen  = testutils.MessageGenerator(queueName)
		consumerMsgGen   = testutils.MessageGenerator(queueName)
		wg               sync.WaitGroup
	)

	pub := pool.NewPublisher(p)
	defer pub.Close()

	// INFO: currently this test allows duplication of messages
	amqputils.ConsumeBatchAsyncN(t, ctx, &wg, hs, queueName, nextConsumerName(), consumerMsgGen, batchSize, numBatches, true)

	// Publish messages in batches
	for i := range numBatches {

		batch := make([]types.Publishing, 0, batchSize)
		for range batchSize {
			msg := publisherMsgGen()
			batch = append(batch, types.Publishing{
				Mandatory:   true,
				ContentType: "text/plain",
				Body:        []byte(msg),
			})
		}

		err = func(batch []types.Publishing) error {
			disconnected, reconnected := proxyutils.DisconnectWithStartedStopped(
				t,
				proxyName,
				0,
				testutils.Jitter(2*time.Millisecond, 5*time.Millisecond),
				testutils.Jitter(300*time.Millisecond, 500*time.Millisecond),
			)
			defer func() {
				disconnected()
				reconnected()
			}()

			return pub.PublishBatch(ctx, exchangeName, "", batch)
		}(batch)
		if !assert.NoError(t, err, fmt.Sprintf("when publishing batch starting at index %d", i)) {
			cancel()
			break
		}
	}
	wg.Wait()
}

/*
// FIXME: TODO: out of memory rabbitmq tests are disabled until https://github.com/rabbitmq/amqp091-go/issues/253 is resolved
func TestPublishAwaitFlowControl(t *testing.T) {
	t.Parallel()

	var (
		ctx          = context.TODO()
		nextConnName = testutils.ConnectionNameGenerator()
	)

	hs, hsclose := NewSession(
		t,
		ctx,
		testutils.HealthyConnectURL,
		nextConnName(),
	)
	defer hsclose()

	p, err := pool.New(
		ctx,
		testutils.BrokenConnectURL,
		1,
		1,
		pool.WithLogger(testlogger.NewTestLogger(t)),
		pool.WithConfirms(true),
	)
	if !assert.NoError(t, err) {
		return
	}
	defer func() {
		p.Close()
	}()
	var (
		nextExchangeName = testutils.ExchangeNameGenerator(hs.Name())
		nextQueueName    = testutils.QueueNameGenerator(hs.Name())
		exchangeName     = nextExchangeName()
		queueName        = nextQueueName()
	)
	ts, err := p.GetTransientSession(ctx)
	if !assert.NoError(t, err) {
		return
	}
	defer func() {
		p.ReturnSession(ts, nil)
	}()
	cleanup := amqputils.DeclareExchangeQueue(t, ctx, ts, exchangeName, queueName)
	defer cleanup()

	var (
		publisherMsgGen = testutils.MessageGenerator(queueName)
	)
	pub := pool.NewPublisher(p)
	defer pub.Close()

	tctx, tcancel := context.WithTimeout(ctx, 10*time.Second)
	defer tcancel()

	err = pub.Publish(tctx, exchangeName, "", types.Publishing{
		ContentType: "text/plain",
		Body:        []byte(publisherMsgGen()),
	})
	if !assert.ErrorIs(t, err, context.DeadlineExceeded) {
		t.Fatal("expected context deadline exceeded error")
	}
	// FIXME: this test gets stuck when the sessions in the session pool are closed.:
}
*/
