package pool_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jxsl13/amqpx/pool"
	"github.com/stretchr/testify/assert"
)

func TestNewSession(t *testing.T) {

	c, err := pool.NewConnection("amqp://admin:password@localhost:5672", "TestNewSession", 1)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer c.Close()

	var wg sync.WaitGroup

	sessions := 200
	wg.Add(sessions)
	for id := 0; id < sessions; id++ {
		go func(id int64) {
			defer wg.Done()
			s, err := pool.NewSession(c, id, pool.SessionWithConfirms(true))
			if err != nil {
				assert.NoError(t, err)
				return
			}
			defer func() {
				assert.NoError(t, s.Close())
			}()

			queueName := fmt.Sprintf("TestNewSession-Queue-%d", id)
			err = s.QueueDeclare(queueName, true, false, false, false, pool.QuorumArgs)
			if err != nil {
				assert.NoError(t, err)
				return
			}
			defer func() {
				i, err := s.QueueDelete(queueName, false, false, false)
				assert.NoError(t, err)
				assert.Equal(t, 0, i)
			}()

			exchangeName := fmt.Sprintf("TestNewSession-Exchange-%d", id)
			err = s.ExchangeDeclare(exchangeName, "fanout", true, false, false, false, nil)
			if err != nil {
				assert.NoError(t, err)
				return
			}
			defer func() {
				err := s.ExchangeDelete(exchangeName, false, false)
				assert.NoError(t, err)
			}()

			err = s.QueueBind(queueName, "#", exchangeName, false, nil)
			if err != nil {
				assert.NoError(t, err)
				return
			}
			defer func() {
				err := s.QueueUnbind(queueName, "#", exchangeName, nil)
				assert.NoError(t, err)
			}()

			delivery, err := s.Consume(queueName, fmt.Sprintf("Consumer-%s", queueName), false, true, false, false, nil)
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

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			tag, err := s.Publish(ctx, exchangeName, "", true, false, pool.Publishing{
				ContentType: "application/json",
				Body:        []byte(message),
			})
			if err != nil {
				assert.NoError(t, err)
				return
			}

			err = s.AwaitConfirm(ctx, tag)
			if err != nil {
				assert.NoError(t, err)
				return
			}

			time.Sleep(5 * time.Second)

		}(int64(id))
	}

	wg.Wait()
}
