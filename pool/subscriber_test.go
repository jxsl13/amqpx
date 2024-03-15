package pool_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jxsl13/amqpx/internal/testutils"
	"github.com/jxsl13/amqpx/logging"
	"github.com/jxsl13/amqpx/pool"
	"github.com/stretchr/testify/assert"
)

func TestNewSingleSubscriber(t *testing.T) {
	t.Parallel()

	var (
		ctx          = context.TODO()
		nextPoolName = testutils.PoolNameGenerator(testutils.FuncName())
		poolName     = nextPoolName()
		hp           = NewPool(t, ctx, testutils.HealthyConnectURL, poolName, 1, 2)
	)
	defer hp.Close()

	var (
		nextExchangeName = testutils.ExchangeNameGenerator(poolName)
		nextQueueName    = testutils.QueueNameGenerator(poolName)
		exchangeName     = nextExchangeName()
		queueName        = nextQueueName()
		nextConsumerName = testutils.ConsumerNameGenerator(queueName)
		consumerName     = nextConsumerName()

		publisherMsgGen  = testutils.MessageGenerator(queueName)
		subscriberMsgGen = testutils.MessageGenerator(queueName)
		numMsgs          = 20
	)

	ts, err := hp.GetTransientSession(ctx)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer hp.ReturnSession(ts, nil)

	cleanup := DeclareExchangeQueue(t, ctx, ts, exchangeName, queueName)
	defer cleanup()
	var wg sync.WaitGroup
	defer wg.Wait()

	SubscriberConsumeAsyncN(t, ctx, &wg, hp, queueName, consumerName, subscriberMsgGen, numMsgs, false)
	PublisherPublishAsyncN(t, ctx, &wg, hp, exchangeName, publisherMsgGen, numMsgs)
}

func TestNewSingleSubscriberWithDisconnect(t *testing.T) {
	t.Parallel()

	var (
		ctx                       = context.TODO()
		nextPoolName              = testutils.PoolNameGenerator(testutils.FuncName())
		poolName                  = nextPoolName()
		hp                        = NewPool(t, ctx, testutils.HealthyConnectURL, poolName, 1, 2)
		proxyName, connectURL, _  = testutils.NextConnectURL()
		pp                        = NewPool(t, ctx, connectURL, nextPoolName()+proxyName, 1, 1)
		disconnected, reconnected = Disconnect(t, proxyName, 5*time.Second)
	)
	defer hp.Close()

	var (
		nextExchangeName = testutils.ExchangeNameGenerator(poolName)
		nextQueueName    = testutils.QueueNameGenerator(poolName)
		exchangeName     = nextExchangeName()
		queueName        = nextQueueName()
		nextConsumerName = testutils.ConsumerNameGenerator(queueName)
		consumerName     = nextConsumerName()

		publisherMsgGen  = testutils.MessageGenerator(queueName)
		subscriberMsgGen = testutils.MessageGenerator(queueName)
		numMsgs          = 20
	)

	ts, err := hp.GetTransientSession(ctx)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer hp.ReturnSession(ts, nil)

	cleanup := DeclareExchangeQueue(t, ctx, ts, exchangeName, queueName)
	defer cleanup()
	var wg sync.WaitGroup
	defer wg.Wait()
	PublisherPublishAsyncN(t, ctx, &wg, hp, exchangeName, publisherMsgGen, numMsgs)

	disconnected()
	defer reconnected()
	SubscriberConsumeN(t, ctx, pp, queueName, consumerName, subscriberMsgGen, numMsgs, true)
}

func TestNewSingleBatchSubscriber(t *testing.T) {
	t.Parallel()

	var (
		ctx          = context.TODO()
		nextPoolName = testutils.PoolNameGenerator(testutils.FuncName())
		poolName     = nextPoolName()
		hp           = NewPool(t, ctx, testutils.HealthyConnectURL, poolName, 1, 2)
	)
	defer hp.Close()

	var (
		nextExchangeName = testutils.ExchangeNameGenerator(poolName)
		nextQueueName    = testutils.QueueNameGenerator(poolName)
		exchangeName     = nextExchangeName()
		queueName        = nextQueueName()
		nextConsumerName = testutils.ConsumerNameGenerator(queueName)
		consumerName     = nextConsumerName()

		publisherMsgGen  = testutils.MessageGenerator(queueName)
		subscriberMsgGen = testutils.MessageGenerator(queueName)
		numMsgs          = 20
		batchSize        = numMsgs / 4
	)

	ts, err := hp.GetTransientSession(ctx)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer hp.ReturnSession(ts, nil)

	cleanup := DeclareExchangeQueue(t, ctx, ts, exchangeName, queueName)
	defer cleanup()
	var wg sync.WaitGroup
	defer wg.Wait()

	SubscriberBatchConsumeAsyncN(
		t,
		ctx,
		&wg,
		hp,
		queueName,
		consumerName,
		subscriberMsgGen,
		numMsgs,
		batchSize,
		numMsgs*1024, // should not be hit
		false,
	)
	PublisherPublishAsyncN(t, ctx, &wg, hp, exchangeName, publisherMsgGen, numMsgs)
}

func TestBatchSubscriberMaxBytes(t *testing.T) {
	t.Parallel()
	var wg sync.WaitGroup
	funcName := testutils.FuncName()

	const (
		maxBatchBytes = 2048
		iterations    = 100
	)
	for i := 0; i < iterations; i++ {
		for j := 1; j <= maxBatchBytes; j = j*2 + 1 {
			wg.Add(1)
			go testBatchSubscriberMaxBytes(t, fmt.Sprintf("%s-%d-max-batch-bytes-%d", funcName, i, j), j, &wg)
		}
	}
	wg.Wait()
}

func testBatchSubscriberMaxBytes(t *testing.T, funcName string, maxBatchBytes int, w *sync.WaitGroup) {
	t.Helper()
	defer w.Done()

	var (
		ctx               = context.TODO()
		nextPoolName      = testutils.PoolNameGenerator(funcName)
		poolName          = nextPoolName()
		nextExchangeQueue = testutils.NewExchangeQueueGenerator(poolName)

		numSessions = 2
		hp          = NewPool(t, ctx, testutils.HealthyConnectURL, poolName, 1, numSessions) // // publisher sessions + consumer sessions

		numMsgs      = 20
		batchTimeout = 5 * time.Second // keep this at a higher number for slow machines
	)
	defer hp.Close()

	var wg sync.WaitGroup

	channels := numSessions / 2 // one sessions for consumer and one for publisher
	wg.Add(channels)
	for id := 0; id < channels; id++ {
		go func(id int64) {
			defer wg.Done()

			var (
				log = logging.NewTestLogger(t)
				eq  = nextExchangeQueue()
			)

			ts, err := hp.GetTransientSession(ctx)
			if err != nil {
				assert.NoError(t, err)
				return
			}
			defer hp.ReturnSession(ts, nil)

			cleanup := DeclareExchangeQueue(t, ctx, ts, eq.Exchange, eq.Queue)
			defer cleanup()

			// publish all messages
			pub := pool.NewPublisher(hp)
			defer pub.Close()

			maxMsgLen := 0
			for i := 0; i < numMsgs; i++ {
				message := fmt.Sprintf("Message-%s-%06d", eq.Queue, i) // max 6 digits
				mlen := len(message)
				if mlen > maxMsgLen {
					maxMsgLen = mlen
				}

				err = pub.Publish(ctx, eq.Exchange, "", pool.Publishing{
					Mandatory:   true,
					ContentType: "text/plain",
					Body:        []byte(message),
				})
				assert.NoError(t, err)
			}
			log.Debugf("max message length: %d", maxMsgLen)
			log.Debugf("max batch bytes: %d", maxBatchBytes)
			expectedMessagesPerBatch := maxBatchBytes / maxMsgLen
			if maxBatchBytes%maxMsgLen > 0 {
				expectedMessagesPerBatch += 1
			}
			log.Debugf("expected messages per batch: %d", expectedMessagesPerBatch)
			expectedBatches := numMsgs / expectedMessagesPerBatch
			if numMsgs%expectedMessagesPerBatch > 0 {
				expectedBatches += 1
			}
			log.Debugf("expected batches: %d", expectedBatches)

			cctx, cancel := context.WithCancel(ctx)
			defer cancel()

			sub := pool.NewSubscriber(hp, pool.SubscriberWithContext(cctx))
			defer sub.Close()

			batchCount := 0
			messageCount := 0
			sub.RegisterBatchHandlerFunc(eq.Queue,
				func(ctx context.Context, msgs []pool.Delivery) error {

					for idx, msg := range msgs {
						assert.Truef(t, len(msg.Body) > 0, "msg body is empty: message index: %d", idx)
						log.Debugf("batch: %d message: %d: body: %q", batchCount, idx, string(msg.Body))
					}

					messageCount += len(msgs)
					batchCount += 1

					expectedMessages := expectedMessagesPerBatch
					if len(msgs)%expectedMessagesPerBatch > 0 {
						expectedMessages = len(msgs) % expectedMessagesPerBatch
					}
					assert.Equal(t, expectedMessages, len(msgs))

					if messageCount == numMsgs {
						// close subscriber from within handler
						cancel()
					}
					return nil
				},
				pool.WithMaxBatchBytes(maxBatchBytes),
				pool.WithMaxBatchSize(0), // disable this check
				pool.WithBatchFlushTimeout(batchTimeout),
				pool.WithBatchConsumeOptions(pool.ConsumeOptions{
					ConsumerTag: eq.ConsumerTag,
					Exclusive:   true,
				}),
			)
			err = sub.Start(ctx)
			assert.NoError(t, err)

			// this should be canceled upon context cancelation from within the
			// subscriber handler.
			sub.Wait()

			assert.Equalf(t, numMsgs, messageCount, "expected messages counter to have the same number as publishes messages")
			assert.Equalf(t, expectedBatches, batchCount, "required to have %d batches received", expectedBatches)

		}(int64(id))
	}

	wg.Wait()
}
