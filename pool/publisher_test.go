package pool_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jxsl13/amqpx/logging"
	"github.com/jxsl13/amqpx/pool"
	"github.com/stretchr/testify/assert"
)

func TestPublisher(t *testing.T) {
	connections := 1
	sessions := 10 // publisher sessions + consumer sessions
	p, err := pool.New("amqp://admin:password@localhost:5672",
		connections,
		sessions,
		pool.WithName("TestPublisher"),
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

			s, err := p.GetSession()
			if err != nil {
				assert.NoError(t, err)
				return
			}

			queueName := fmt.Sprintf("TestPublisher-Queue-%d", id)
			_, err = s.QueueDeclare(queueName)
			if err != nil {
				assert.NoError(t, err)
				return
			}
			defer func() {
				i, err := s.QueueDelete(queueName)
				assert.NoError(t, err)
				assert.Equal(t, 0, i)
			}()

			exchangeName := fmt.Sprintf("TestPublisher-Exchange-%d", id)
			err = s.ExchangeDeclare(exchangeName, "topic")
			if err != nil {
				assert.NoError(t, err)
				return
			}
			defer func() {
				err := s.ExchangeDelete(exchangeName)
				assert.NoError(t, err)
			}()

			err = s.QueueBind(queueName, "#", exchangeName)
			if err != nil {
				assert.NoError(t, err)
				return
			}
			defer func() {
				err := s.QueueUnbind(queueName, "#", exchangeName, nil)
				assert.NoError(t, err)
			}()

			delivery, err := s.Consume(
				queueName,
				pool.ConsumeOptions{
					ConsumerTag: fmt.Sprintf("Consumer-%s", queueName),
					Exclusive:   true,
				},
			)
			if err != nil {
				assert.NoError(t, err)
				return
			}

			message := fmt.Sprintf("Message-%s", queueName)

			wg.Add(1)
			go func(msg string) {
				defer wg.Done()

				for val := range delivery {
					receivedMsg := string(val.Body)
					assert.Equal(t, message, receivedMsg)
				}
				// this routine must be closed upon session closure
			}(message)

			time.Sleep(5 * time.Second)

			pub := pool.NewPublisher(p)
			defer pub.Close()

			pub.Publish(exchangeName, "", pool.Publishing{
				Mandatory:   true,
				ContentType: "application/json",
				Body:        []byte(message),
			})

			time.Sleep(5 * time.Second)

		}(int64(id))
	}

	wg.Wait()
}
