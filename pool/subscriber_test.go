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
