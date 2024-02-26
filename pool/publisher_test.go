package pool_test

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"testing"
	"time"

	"github.com/jxsl13/amqpx/logging"
	"github.com/jxsl13/amqpx/pool"
	"github.com/stretchr/testify/assert"
)

func TestPublisher(t *testing.T) {
	ctx := context.TODO()
	connections := 1
	sessions := 10 // publisher sessions + consumer sessions
	p, err := pool.New(
		ctx,
		connectURL,
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

			s, err := p.GetSession(ctx)
			if err != nil {
				assert.NoError(t, err)
				return
			}
			defer func() {
				p.ReturnSession(ctx, s, false)
			}()

			queueName := fmt.Sprintf("TestPublisher-Queue-%d", id)
			_, err = s.QueueDeclare(ctx, queueName)
			if err != nil {
				assert.NoError(t, err)
				return
			}
			defer func() {
				i, err := s.QueueDelete(ctx, queueName)
				assert.NoError(t, err)
				assert.Equal(t, 0, i)
			}()

			exchangeName := fmt.Sprintf("TestPublisher-Exchange-%d", id)
			err = s.ExchangeDeclare(ctx, exchangeName, "topic")
			if err != nil {
				assert.NoError(t, err)
				return
			}
			defer func() {
				err := s.ExchangeDelete(ctx, exchangeName)
				assert.NoError(t, err)
			}()

			err = s.QueueBind(ctx, queueName, "#", exchangeName)
			if err != nil {
				assert.NoError(t, err)
				return
			}
			defer func() {
				err := s.QueueUnbind(ctx, queueName, "#", exchangeName, nil)
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

			pub.Publish(ctx, exchangeName, "", pool.Publishing{
				Mandatory:   true,
				ContentType: "application/json",
				Body:        []byte(message),
			})

			time.Sleep(5 * time.Second)

		}(int64(id))
	}

	wg.Wait()
}

func TestPauseOnFlowControl(t *testing.T) {
	ctx, cancel := signal.NotifyContext(context.TODO(), os.Interrupt)
	defer cancel()

	connections := 1
	sessions := 2 // publisher sessions + consumer sessions
	p, err := pool.New(
		ctx,
		brokenConnectURL, //
		connections,
		sessions,
		pool.WithName("TestPauseOnFlowControl"),
		pool.WithConfirms(true),
		pool.WithLogger(logging.NewTestLogger(t)),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer p.Close()

	s, err := p.GetSession(ctx)
	if err != nil {
		assert.NoError(t, err)
		return
	}

	var (
		exchangeName = "TestPauseOnFlowControl-Exchange"
	)

	cleanup := initQueueExchange(t, s, ctx, "TestPauseOnFlowControl-Queue", exchangeName)
	defer cleanup()

	pub := pool.NewPublisher(p)
	defer pub.Close()

	pubGen := PublishingGenerator("TestPauseOnFlowControl")

	err = pub.Publish(ctx, exchangeName, "", pubGen())
	assert.NoError(t, err)

}

func initQueueExchange(t *testing.T, s *pool.Session, ctx context.Context, queueName, exchangeName string) (cleanup func()) {
	_, err := s.QueueDeclare(ctx, queueName)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	cleanupList := []func(){}

	cleanupQueue := func() {
		i, err := s.QueueDelete(ctx, queueName)
		assert.NoError(t, err)
		assert.Equal(t, 0, i)
	}
	cleanupList = append(cleanupList, cleanupQueue)

	err = s.ExchangeDeclare(ctx, exchangeName, "topic")
	if err != nil {
		assert.NoError(t, err)
		return
	}
	cleanupExchange := func() {
		err := s.ExchangeDelete(ctx, exchangeName)
		assert.NoError(t, err)
	}
	cleanupList = append(cleanupList, cleanupExchange)

	err = s.QueueBind(ctx, queueName, "#", exchangeName)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	cleanupBind := func() {
		err := s.QueueUnbind(ctx, queueName, "#", exchangeName, nil)
		assert.NoError(t, err)
	}
	cleanupList = append(cleanupList, cleanupBind)

	return func() {
		for i := len(cleanupList) - 1; i >= 0; i-- {
			cleanupList[i]()
		}
	}
}

func PublishingGenerator(MessagePrefix string) func() pool.Publishing {
	i := 0
	return func() pool.Publishing {
		defer func() {
			i++
		}()
		return pool.Publishing{
			ContentType: "application/json",
			Body:        []byte(fmt.Sprintf("%s-%d", MessagePrefix, i)),
		}
	}
}
