package pool_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jxsl13/amqpx/logging"
	"github.com/jxsl13/amqpx/pool"
	"github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
)

func TestSubscriber(t *testing.T) {

	sessions := 2 // publisher sessions + consumer sessions
	p, err := pool.New("amqp://admin:password@localhost:5672", 1, sessions, pool.WithConfirms(true), pool.WithLogger(logging.NewTestLogger(t)))
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
			defer p.ReturnSession(ts, false)

			queueName := fmt.Sprintf("TestSubscriber-Queue-%d", id)
			err = ts.QueueDeclare(queueName)
			if err != nil {
				assert.NoError(t, err)
				return
			}
			defer func() {
				i, err := ts.QueueDelete(queueName)
				assert.NoError(t, err)
				assert.Equal(t, 0, i)
			}()

			exchangeName := fmt.Sprintf("TestSubscriber-Exchange-%d", id)
			err = ts.ExchangeDeclare(exchangeName, "topic")
			if err != nil {
				assert.NoError(t, err)
				return
			}
			defer func() {
				err := ts.ExchangeDelete(exchangeName)
				assert.NoError(t, err)
			}()

			err = ts.QueueBind(queueName, "#", exchangeName)
			if err != nil {
				assert.NoError(t, err)
				return
			}
			defer func() {
				err := ts.QueueUnbind(queueName, "#", exchangeName, nil)
				assert.NoError(t, err)
			}()

			message := fmt.Sprintf("Message-%s", queueName)

			ctx, cancel := context.WithCancel(p.Context())

			sub := pool.NewSubscriber(p, pool.SubscriberWithContext(ctx))
			defer sub.Close()

			sub.RegisterHandlerFunc(queueName,
				func(msg amqp091.Delivery) error {

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
			sub.Start()
			time.Sleep(5 * time.Second)

			pub := pool.NewPublisher(p)
			defer pub.Close()

			pub.Publish(exchangeName, "", pool.Publishing{
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
		sessions     = 2 // publisher sessions + consumer sessions
		numMessages  = 50
		batchTimeout = 10 * time.Second // keep this at a higher number for slow machines
	)
	p, err := pool.New("amqp://admin:password@localhost:5672", 1, sessions, pool.WithConfirms(true), pool.WithLogger(logging.NewTestLogger(t)))
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
			defer p.ReturnSession(ts, false)

			queueName := fmt.Sprintf("TestBatchSubscriber-Queue-%d", id)
			err = ts.QueueDeclare(queueName)
			if err != nil {
				assert.NoError(t, err)
				return
			}
			defer func() {
				i, err := ts.QueueDelete(queueName)
				assert.NoError(t, err)
				assert.Equal(t, 0, i)
			}()

			exchangeName := fmt.Sprintf("TestBatchSubscriber-Exchange-%d", id)
			err = ts.ExchangeDeclare(exchangeName, "topic")
			if err != nil {
				assert.NoError(t, err)
				return
			}
			defer func() {
				err := ts.ExchangeDelete(exchangeName)
				assert.NoError(t, err)
			}()

			err = ts.QueueBind(queueName, "#", exchangeName)
			if err != nil {
				assert.NoError(t, err)
				return
			}
			defer func() {
				err := ts.QueueUnbind(queueName, "#", exchangeName, nil)
				assert.NoError(t, err)
			}()

			// publish all messages
			pub := pool.NewPublisher(p)
			defer pub.Close()

			for i := 0; i < numMessages; i++ {
				message := fmt.Sprintf("Message-%s-%d", queueName, i)

				pub.Publish(exchangeName, "", pool.Publishing{
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
				func(msgs []amqp091.Delivery) error {
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
			sub.Start()

			// this should be canceled upon context cancelation from within the
			// subscriber handler.
			sub.Wait()

			assert.Equalf(t, numMessages, messageCount, "expected messages counter to have the same number as publishes messages")
			assert.Equalf(t, 2, batchCount, "required to have two batches received")

		}(int64(id))
	}

	wg.Wait()
}
