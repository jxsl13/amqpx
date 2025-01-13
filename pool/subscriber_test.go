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
	amqp "github.com/rabbitmq/amqp091-go"
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

// This test proofs that Qos and prefetch_count are not required when consuming batches.
func TestLowLevelConsumeBatchOK(t *testing.T) {
	t.Parallel()
	var (
		funcName = testutils.FuncName()
	)

	tests := []struct {
		Name         string
		Qos          bool
		MessageCount int
		BatchSize    int
		BatchTimeout time.Duration
	}{
		{"#1", true, 10, 2, 100 * time.Millisecond},
		{"#2", true, 10, 20, 100 * time.Millisecond},
		{"#3", true, 10, 2, 500 * time.Millisecond},
		{"#4", true, 10, 20, 500 * time.Millisecond},
		{"#5", false, 10, 2, 100 * time.Millisecond},
		{"#6", false, 10, 20, 100 * time.Millisecond},
		{"#7", false, 10, 2, 500 * time.Millisecond},
		{"#8", false, 10, 20, 500 * time.Millisecond},
	}

	for _, test := range tests {

		test := test
		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()

			var (
				log               = logging.NewTestLogger(t)
				ctx, cancel       = context.WithCancel(context.TODO())
				nextExchangeQueue = testutils.NewExchangeQueueGenerator(fmt.Sprintf("%s_%s", funcName, test.Name))
				eq                = nextExchangeQueue()
				exchangeName      = eq.Exchange
				queueName         = eq.Queue

				messageCount = test.MessageCount
				batchSize    = test.BatchSize
				batchTimeout = test.BatchTimeout
				chDone       = make(chan struct{})
				chAck        = make(chan struct{})
			)

			log.SetLevel(0)
			defer cancel()

			// open connection
			conn, err := amqp.Dial(testutils.HealthyConnectURL)
			if err != nil {
				log.Fatal(err)
			}
			defer conn.Close()

			// open channel
			ch, err := conn.Channel()
			if err != nil {
				log.Fatal(err)
			}
			defer ch.Close()

			// declare queue
			err = ch.ExchangeDeclare(exchangeName, string(pool.ExchangeKindFanOut), true, false, false, false, nil)
			if err != nil {
				assert.NoError(t, err)
				return
			}
			defer ch.ExchangeDelete(exchangeName, false, false)

			q, err := ch.QueueDeclare(queueName, true, false, false, true, nil)
			if err != nil {
				assert.NoError(t, err)
				return
			}
			defer ch.QueueDelete(q.Name, false, false, true)

			// publish message
			for i := 0; i < messageCount; i++ {
				err = ch.PublishWithContext(ctx, "", q.Name, false, false, amqp.Publishing{
					ContentType: "text/plain",
					Body:        []byte(eq.NextPubMsg()),
				})
				if err != nil {
					assert.NoError(t, err)
					return
				}
			}

			var wg sync.WaitGroup

			// consume message in batch but do not acknowledge
			wg.Add(1)
			go func() {
				defer wg.Done()
				consume(log, test.Qos, batchSize, batchTimeout, chDone, chAck, conn, queueName, messageCount, func(batch []*amqp.Delivery) {
					for i, d := range batch {
						log.Infof("Processing nack message: %d: %s", i, string(d.Body))
					}
				})
			}()

			// let it run for a while
			time.Sleep(4200 * time.Millisecond)

			// close the consumer
			close(chDone)
			wg.Wait()

			// re-open the consumer, this time acknowledge the messages
			chDone = make(chan struct{})
			close(chAck)

			// consume message one by one
			wg.Add(1)
			go func() {
				defer wg.Done()
				consume(log, test.Qos, 1, batchTimeout, chDone, chAck, conn, queueName, messageCount, func(batch []*amqp.Delivery) {
					for i, d := range batch {
						log.Infof("Processing ack message: %d: %s", i, string(d.Body))
						err := eq.ValidateNextSubMsg(t, string(d.Body))
						assert.NoError(t, err)

					}
				})
			}()

			select {
			case <-chDone:
				log.Info("consumed successfully")
			case <-time.After(time.Duration(test.MessageCount) * 300 * time.Millisecond):
				t.Error("failed to consume all messages: timeout reached")
			}
		})
	}

}

// This test proofs that Qos and prefetch_count are not required when consuming batches.
func TestLowLevelConsumeBatchOK_2(t *testing.T) {
	t.Parallel()
	var (
		funcName = testutils.FuncName()
	)

	tests := []struct {
		Name         string
		Qos          bool
		MessageCount int
		BatchSize    int
		BatchTimeout time.Duration
	}{
		{"#1", true, 10, 2, 50 * time.Millisecond},
		{"#2", true, 10, 20, 100 * time.Millisecond},
		{"#3", true, 10, 2, 500 * time.Millisecond},
		{"#4", true, 10, 20, 500 * time.Millisecond},
		{"#5", false, 10, 2, 50 * time.Millisecond},
		{"#6", false, 10, 20, 100 * time.Millisecond},
		{"#7", false, 10, 2, 500 * time.Millisecond},
		{"#8", false, 10, 20, 500 * time.Millisecond},
	}

	for _, test := range tests {

		test := test
		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()

			var (
				log               = logging.NewTestLogger(t)
				ctx, cancel       = context.WithCancel(context.TODO())
				nextExchangeQueue = testutils.NewExchangeQueueGenerator(fmt.Sprintf("%s_%s", funcName, test.Name))
				eq                = nextExchangeQueue()
				exchangeName      = eq.Exchange
				queueName         = eq.Queue

				messageCount = test.MessageCount
				batchSize    = test.BatchSize
				batchTimeout = test.BatchTimeout
				chDone       = make(chan struct{})
				chAck        = make(chan struct{})
			)

			log.SetLevel(0)
			defer cancel()

			// open connection
			conn, err := amqp.Dial(testutils.HealthyConnectURL)
			if err != nil {
				log.Fatal(err)
			}
			defer conn.Close()

			// open channel
			ch, err := conn.Channel()
			if err != nil {
				log.Fatal(err)
			}
			defer ch.Close()

			// declare queue
			err = ch.ExchangeDeclare(exchangeName, string(pool.ExchangeKindFanOut), true, false, false, false, nil)
			if err != nil {
				assert.NoError(t, err)
				return
			}
			defer ch.ExchangeDelete(exchangeName, false, false)

			q, err := ch.QueueDeclare(queueName, true, false, false, true, nil)
			if err != nil {
				assert.NoError(t, err)
				return
			}
			defer ch.QueueDelete(q.Name, false, false, true)

			var wg sync.WaitGroup

			// consume message in batch but do not acknowledge
			wg.Add(1)
			go func() {
				defer wg.Done()
				consume(log, test.Qos, batchSize, batchTimeout, chDone, chAck, conn, queueName, messageCount, func(batch []*amqp.Delivery) {
					for i, d := range batch {
						log.Infof("Processing nack message: %d: %s", i, string(d.Body))
					}
				})
			}()

			// publish message
			for i := 0; i < messageCount; i++ {
				err = ch.PublishWithContext(ctx, "", q.Name, false, false, amqp.Publishing{
					ContentType: "text/plain",
					Body:        []byte(eq.NextPubMsg()),
				})
				if err != nil {
					assert.NoError(t, err)
					return
				}
			}
			// let it run for a while
			time.Sleep(4200 * time.Millisecond)

			// close the consumer
			close(chDone)
			wg.Wait()

			// re-open the consumer, this time acknowledge the messages
			chDone = make(chan struct{})
			close(chAck)

			// consume message one by one
			wg.Add(1)
			go func() {
				defer wg.Done()
				consume(log, test.Qos, 1, batchTimeout, chDone, chAck, conn, queueName, messageCount, func(batch []*amqp.Delivery) {
					for i, d := range batch {
						log.Infof("Processing ack message: %d: %s", i, string(d.Body))

						err := eq.ValidateNextSubMsg(t, string(d.Body))
						assert.NoError(t, err)
					}
				})
			}()

			select {
			case <-chDone:
				log.Info("consumed successfully")
			case <-time.After(time.Duration(test.MessageCount) * 300 * time.Millisecond):
				t.Error("failed to consume all messages: timeout reached")
			}
		})
	}
}

func consume(log logging.Logger, qos bool, batchSize int, batchTimeout time.Duration, chDone chan struct{}, chAck chan struct{}, conn *amqp.Connection, queueName string, messageCount int, process func(batch []*amqp.Delivery)) {
	defer func() {
		log.Info("consumer closed")
	}()

	log.Info("starting consumer...")
	ch, err := conn.Channel()
	if err != nil {
		log.Error(err)
		return
	}
	defer ch.Close()

	if qos {
		// consume message
		err = ch.Qos(batchSize, 0, false)
		if err != nil {
			log.Error(err)
			return
		}
	}

	msgs, err := ch.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		log.Error(err)
		return
	}

	log.Info("consumer started")
	counter := 0

all:
	for {
		batch := make([]*amqp.Delivery, 0, batchSize)
		batchCounter := 0
		c := time.After(batchTimeout)

	collect:
		for {
			select {
			case d, ok := <-msgs:
				if !ok {
					log.Error("delivery channel closed")
					return
				}

				batch = append(batch, &d)
				batchCounter++
				if batchCounter >= batchSize {
					log.Infof("Batch is full with %d messages", len(batch))
					break collect
				}
			case <-c:
				log.Infof("Batch timeout with %d messages", len(batch))
				break collect
			case <-chDone:
				break all
			}
		}

		if len(batch) == 0 {
			log.Info("Empty batch, skipping processing")
			continue
		}

		select {
		case <-chAck:
			process(batch)

			err := batch[len(batch)-1].Ack(true)
			if err != nil {
				log.Error(err)
				return
			}

			counter += len(batch)
			if counter >= messageCount {
				close(chDone)
			}
		default:
			process(batch)

			err := batch[len(batch)-1].Nack(true, true)
			if err != nil {
				log.Error(err)
				return
			}
		}
	}

}
