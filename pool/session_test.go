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

func TestNewSession(t *testing.T) {

	c, err := pool.NewConnection(
		"amqp://admin:password@localhost:5672",
		"TestNewSession",
		pool.ConnectionWithLogger(logging.NewTestLogger(t)),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer c.Close()

	var wg sync.WaitGroup

	sessions := 5
	wg.Add(sessions)
	for id := 0; id < sessions; id++ {
		go func(id int64) {
			defer wg.Done()
			s, err := pool.NewSession(c, fmt.Sprintf("TestNewSession-%d", id), pool.SessionWithConfirms(true))
			if err != nil {
				assert.NoError(t, err)
				return
			}
			defer func() {
				assert.NoError(t, s.Close())
			}()

			queueName := fmt.Sprintf("TestNewSession-Queue-%d", id)
			err = s.QueueDeclare(queueName, true, false, false, false, QuorumArgs)
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
			err = s.ExchangeDeclare(exchangeName, "topic", true, false, false, false, nil)
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

				msgsReceived := 0
				for val := range delivery {
					receivedMsg := string(val.Body)
					assert.Equal(t, message, receivedMsg)
					msgsReceived++
				}
				assert.Equal(t, 1, msgsReceived)
				// this routine must be closed upon session closure
			}(message)

			time.Sleep(5 * time.Second)

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			tag, err := s.Publish(ctx, exchangeName, "", true, false, amqp091.Publishing{
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

func TestNewSessionDisconnect(t *testing.T) {

	c, err := pool.NewConnection(
		"amqp://admin:password@localhost:5672",
		"TestNewSessionDisconnect",
		pool.ConnectionWithLogger(logging.NewTestLogger(t)),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer func() {
		c.Close() // can be nil or error
	}()

	var wg sync.WaitGroup

	sessions := 1
	wg.Add(sessions)

	start, started, stopped := DisconnectWithStartStartedStopped(t, time.Second)
	start2, started2, stopped2 := DisconnectWithStartStartedStopped(t, time.Second)
	start3, started3, stopped3 := DisconnectWithStartStartedStopped(t, time.Second)
	start4, started4, stopped4 := DisconnectWithStartStartedStopped(t, time.Second)
	start5, started5, stopped5 := DisconnectWithStartStartedStopped(t, time.Second)
	start6, started6, stopped6 := DisconnectWithStartStartedStopped(t, time.Second)

	// deferred
	start7, started7, stopped7 := DisconnectWithStartStartedStopped(t, time.Second)
	start8, started8, stopped8 := DisconnectWithStartStartedStopped(t, time.Second)
	start9, started9, stopped9 := DisconnectWithStartStartedStopped(t, time.Second)
	start10, started10, stopped10 := DisconnectWithStartStartedStopped(t, time.Second)

	for id := 0; id < sessions; id++ {
		go func(id int64) {
			defer wg.Done()
			s, err := pool.NewSession(c, fmt.Sprintf("TestNewSession-%d", id), pool.SessionWithConfirms(true))
			if err != nil {
				assert.NoError(t, err)
				return
			}
			defer func() {
				start10()
				started10()

				s.Close() // can be nil or error
				stopped10()
			}()

			start() // await connection loss start
			started()

			exchangeName := fmt.Sprintf("TestNewSession-Exchange-%d", id)
			err = s.ExchangeDeclare(exchangeName, "topic", true, false, false, false, nil)
			if err != nil {
				assert.NoError(t, err)
				return
			}

			stopped()

			defer func() {
				start9()
				started9()

				err := s.ExchangeDelete(exchangeName, false, false)
				assert.NoError(t, err)

				stopped9()
			}()

			start2()
			started2()

			queueName := fmt.Sprintf("TestNewSession-Queue-%d", id)
			err = s.QueueDeclare(queueName, true, false, false, false, QuorumArgs)
			if err != nil {
				assert.NoError(t, err)
				return
			}
			stopped2()

			defer func() {
				start8()
				started8()
				stopped8()

				i, err := s.QueueDelete(queueName, false, false, false)
				assert.NoError(t, err)
				assert.Equal(t, 0, i)

			}()

			start3()
			started3()

			err = s.QueueBind(queueName, "#", exchangeName, false, nil)
			if err != nil {
				assert.NoError(t, err)
				return
			}
			stopped3()

			defer func() {
				start7()
				started7()

				err := s.QueueUnbind(queueName, "#", exchangeName, nil)
				assert.NoError(t, err)

				stopped7()
			}()

			start4()
			started4()

			delivery, err := s.Consume(queueName, fmt.Sprintf("Consumer-%s", queueName), false, true, false, false, nil)
			if err != nil {
				assert.NoError(t, err)
				return
			}
			stopped4()

			message := fmt.Sprintf("Message-%s", queueName)

			wg.Add(1)
			go func(msg string) {
				defer wg.Done()

				msgsReceived := 0
				for val := range delivery {
					receivedMsg := string(val.Body)
					assert.Equal(t, message, receivedMsg)
					msgsReceived++
				}
				// consumption fails because the connection will be closed
				assert.Equal(t, 0, msgsReceived)
				// this routine must be closed upon session closure
			}(message)

			time.Sleep(2 * time.Second)

			start5()
			started5()
			var once sync.Once

			for {
				tag, err := s.Publish(context.Background(), exchangeName, "", true, false, amqp091.Publishing{
					ContentType: "application/json",
					Body:        []byte(message),
				})
				if err != nil {
					assert.NoError(t, err)
					return
				}

				stopped5()

				once.Do(func() {
					start6()
					started6()
					stopped6()
				})

				err = s.AwaitConfirm(context.Background(), tag)
				if err != nil {
					// retry because the first attempt at confirmation failed
					continue
				}
				break
			}

		}(int64(id))
	}

	wg.Wait()
}
