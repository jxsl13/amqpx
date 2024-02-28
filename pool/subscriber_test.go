package pool_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jxsl13/amqpx/logging"
	"github.com/jxsl13/amqpx/pool"
	"github.com/stretchr/testify/assert"
)

func TestSubscriber(t *testing.T) {
	ctx := context.TODO()
	sessions := 2 // publisher sessions + consumer sessions
	p, err := pool.New(
		ctx,
		connectURL,
		1,
		sessions,
		pool.WithConfirms(true),
		pool.WithLogger(logging.NewTestLogger(t)),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer p.Close()

	var wg sync.WaitGroup

	channels := sessions / 2 // one sessions for consumer and one for publisher
	wg.Add(channels)
	for id := 0; id < channels; id++ {
		go func(id int64) {
			defer wg.Done()

			ts, err := p.GetTransientSession(p.Context())
			if err != nil {
				assert.NoError(t, err)
				return
			}
			defer p.ReturnSession(ts, nil)

			queueName := fmt.Sprintf("TestSubscriber-Queue-%d", id)
			_, err = ts.QueueDeclare(ctx, queueName)
			if err != nil {
				assert.NoError(t, err)
				return
			}
			defer func() {
				i, err := ts.QueueDelete(ctx, queueName)
				assert.NoError(t, err)
				assert.Equal(t, 0, i)
			}()

			exchangeName := fmt.Sprintf("TestSubscriber-Exchange-%d", id)
			err = ts.ExchangeDeclare(ctx, exchangeName, "topic")
			if err != nil {
				assert.NoError(t, err)
				return
			}
			defer func() {
				err := ts.ExchangeDelete(ctx, exchangeName)
				assert.NoError(t, err)
			}()

			err = ts.QueueBind(ctx, queueName, "#", exchangeName)
			if err != nil {
				assert.NoError(t, err)
				return
			}
			defer func() {
				err := ts.QueueUnbind(ctx, queueName, "#", exchangeName, nil)
				assert.NoError(t, err)
			}()

			message := fmt.Sprintf("Message-%s", queueName)

			cctx, cancel := context.WithCancel(p.Context())

			sub := pool.NewSubscriber(p, pool.SubscriberWithContext(cctx))
			defer sub.Close()

			sub.RegisterHandlerFunc(queueName,
				func(ctx context.Context, msg pool.Delivery) error {

					// handler func
					receivedMsg := string(msg.Body)
					// assert equel to message that is to be sent
					assert.Equal(t, message, receivedMsg)

					// close subscriber from within handler
					cancel()
					return nil
				},
				pool.ConsumeOptions{
					ConsumerTag: fmt.Sprintf("Consumer-%s", queueName),
					Exclusive:   true,
				},
			)
			err = sub.Start(ctx)
			if err != nil {
				assert.NoError(t, err)
				return
			}
			time.Sleep(5 * time.Second)

			pub := pool.NewPublisher(p)
			defer pub.Close()

			pub.Publish(ctx, exchangeName, "", pool.Publishing{
				Mandatory:   true,
				ContentType: "application/json",
				Body:        []byte(message),
			})

			// this should be canceled upon context cancelation from within the
			// subscriber handler.
			sub.Wait()

		}(int64(id))
	}

	wg.Wait()
}

func TestBatchSubscriber(t *testing.T) {
	var (
		ctx          = context.TODO()
		sessions     = 2 // publisher sessions + consumer sessions
		numMessages  = 50
		batchTimeout = 10 * time.Second // keep this at a higher number for slow machines
	)
	p, err := pool.New(
		ctx,
		connectURL,
		1,
		sessions,
		pool.WithConfirms(true),
		pool.WithLogger(logging.NewTestLogger(t)),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer p.Close()

	var wg sync.WaitGroup

	channels := sessions / 2 // one sessions for consumer and one for publisher
	wg.Add(channels)
	for id := 0; id < channels; id++ {
		go func(id int64) {
			defer wg.Done()

			ts, err := p.GetTransientSession(p.Context())
			if err != nil {
				assert.NoError(t, err)
				return
			}
			defer p.ReturnSession(ts, nil)

			queueName := fmt.Sprintf("TestBatchSubscriber-Queue-%d", id)
			_, err = ts.QueueDeclare(ctx, queueName)
			if err != nil {
				assert.NoError(t, err)
				return
			}
			defer func() {
				i, err := ts.QueueDelete(ctx, queueName)
				assert.NoError(t, err)
				assert.Equal(t, 0, i)
			}()

			exchangeName := fmt.Sprintf("TestBatchSubscriber-Exchange-%d", id)
			err = ts.ExchangeDeclare(ctx, exchangeName, "topic")
			if err != nil {
				assert.NoError(t, err)
				return
			}
			defer func() {
				err := ts.ExchangeDelete(ctx, exchangeName)
				assert.NoError(t, err)
			}()

			err = ts.QueueBind(ctx, queueName, "#", exchangeName)
			if err != nil {
				assert.NoError(t, err)
				return
			}
			defer func() {
				err := ts.QueueUnbind(ctx, queueName, "#", exchangeName, nil)
				assert.NoError(t, err)
			}()

			// publish all messages
			pub := pool.NewPublisher(p)
			defer pub.Close()

			for i := 0; i < numMessages; i++ {
				message := fmt.Sprintf("Message-%s-%d", queueName, i)

				pub.Publish(ctx, exchangeName, "", pool.Publishing{
					Mandatory:   true,
					ContentType: "application/json",
					Body:        []byte(message),
				})
			}

			ctx, cancel := context.WithCancel(p.Context())

			sub := pool.NewSubscriber(p, pool.SubscriberWithContext(ctx))
			defer sub.Close()

			batchSize := numMessages / 2

			batchCount := 0
			messageCount := 0
			sub.RegisterBatchHandlerFunc(queueName,
				func(ctx context.Context, msgs []pool.Delivery) error {
					log := logging.NewTestLogger(t)
					assert.Equal(t, batchSize, len(msgs))

					for idx, msg := range msgs {
						assert.Truef(t, len(msg.Body) > 0, "msg body is empty: message index: %d", idx)
						log.Debugf("batch: %d message: %d: body: %q", batchCount, idx, string(msg.Body))
					}

					messageCount += len(msgs)
					batchCount += 1

					if messageCount == numMessages {
						// close subscriber from within handler
						cancel()
					}
					return nil
				},
				pool.WithMaxBatchSize(batchSize),
				pool.WithBatchFlushTimeout(batchTimeout),
				pool.WithBatchConsumeOptions(pool.ConsumeOptions{
					ConsumerTag: fmt.Sprintf("Consumer-%s", queueName),
					Exclusive:   true,
				}),
			)
			sub.Start(ctx)

			// this should be canceled upon context cancelation from within the
			// subscriber handler.
			sub.Wait()

			assert.Equalf(t, numMessages, messageCount, "expected messages counter to have the same number as publishes messages")
			assert.Equalf(t, 2, batchCount, "required to have two batches received")

		}(int64(id))
	}

	wg.Wait()
}

func TestBatchSubscriberMaxBytes(t *testing.T) {
	for i := 1; i <= 2048; i = i*2 + 1 {
		testBatchSubscriberMaxBytes(t, i)
	}
}

func testBatchSubscriberMaxBytes(t *testing.T, maxBatchBytes int) {
	t.Helper()

	var (
		ctx          = context.TODO()
		sessions     = 2 // publisher sessions + consumer sessions
		numMessages  = 50
		batchTimeout = 5 * time.Second // keep this at a higher number for slow machines
	)
	p, err := pool.New(
		ctx,
		connectURL,
		1,
		sessions,
		pool.WithConfirms(true),
		pool.WithLogger(logging.NewTestLogger(t)),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer p.Close()

	var wg sync.WaitGroup

	channels := sessions / 2 // one sessions for consumer and one for publisher
	wg.Add(channels)
	for id := 0; id < channels; id++ {
		go func(id int64) {
			defer wg.Done()

			ts, err := p.GetTransientSession(p.Context())
			if err != nil {
				assert.NoError(t, err)
				return
			}
			defer p.ReturnSession(ts, nil)

			queueName := fmt.Sprintf("TestBatchSubscriberMaxBytes-Queue-%d", id)
			_, err = ts.QueueDeclare(ctx, queueName)
			if err != nil {
				assert.NoError(t, err)
				return
			}
			defer func() {
				i, err := ts.QueueDelete(ctx, queueName)
				assert.NoError(t, err)
				assert.Equal(t, 0, i)
			}()

			exchangeName := fmt.Sprintf("TestBatchSubscriberMaxBytes-Exchange-%d", id)
			err = ts.ExchangeDeclare(ctx, exchangeName, "topic")
			if err != nil {
				assert.NoError(t, err)
				return
			}
			defer func() {
				err := ts.ExchangeDelete(ctx, exchangeName)
				assert.NoError(t, err)
			}()

			err = ts.QueueBind(ctx, queueName, "#", exchangeName)
			if err != nil {
				assert.NoError(t, err)
				return
			}
			defer func() {
				err := ts.QueueUnbind(ctx, queueName, "#", exchangeName, nil)
				assert.NoError(t, err)
			}()

			// publish all messages
			pub := pool.NewPublisher(p)
			defer pub.Close()

			log := logging.NewTestLogger(t)

			maxMsgLen := 0
			for i := 0; i < numMessages; i++ {
				message := fmt.Sprintf("Message-%s-%06d", queueName, i) // max 6 digits
				mlen := len(message)
				if mlen > maxMsgLen {
					maxMsgLen = mlen
				}

				pub.Publish(ctx, exchangeName, "", pool.Publishing{
					Mandatory:   true,
					ContentType: "application/json",
					Body:        []byte(message),
				})
			}
			log.Debugf("max message length: %d", maxMsgLen)
			log.Debugf("max batch bytes: %d", maxBatchBytes)
			expectedMessagesPerBatch := maxBatchBytes / maxMsgLen
			if maxBatchBytes%maxMsgLen > 0 {
				expectedMessagesPerBatch += 1
			}
			log.Debugf("expected messages per batch: %d", expectedMessagesPerBatch)
			expectedBatches := numMessages / expectedMessagesPerBatch
			if numMessages%expectedMessagesPerBatch > 0 {
				expectedBatches += 1
			}
			log.Debugf("expected batches: %d", expectedBatches)

			cctx, cancel := context.WithCancel(p.Context())

			sub := pool.NewSubscriber(p, pool.SubscriberWithContext(cctx))
			defer sub.Close()

			batchCount := 0
			messageCount := 0
			sub.RegisterBatchHandlerFunc(queueName,
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

					if messageCount == numMessages {
						// close subscriber from within handler
						cancel()
					}
					return nil
				},
				pool.WithMaxBatchBytes(maxBatchBytes),
				pool.WithMaxBatchSize(0), // disable this check
				pool.WithBatchFlushTimeout(batchTimeout),
				pool.WithBatchConsumeOptions(pool.ConsumeOptions{
					ConsumerTag: fmt.Sprintf("Consumer-%s", queueName),
					Exclusive:   true,
				}),
			)
			sub.Start(ctx)

			// this should be canceled upon context cancelation from within the
			// subscriber handler.
			sub.Wait()

			assert.Equalf(t, numMessages, messageCount, "expected messages counter to have the same number as publishes messages")
			assert.Equalf(t, expectedBatches, batchCount, "required to have %d batches received", expectedBatches)

		}(int64(id))
	}

	wg.Wait()
}
