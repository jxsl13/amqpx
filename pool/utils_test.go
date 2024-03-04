package pool_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jxsl13/amqpx/pool"
	"github.com/stretchr/testify/assert"
)

func ConsumeAsyncN(
	t *testing.T,
	ctx context.Context,
	wg *sync.WaitGroup,
	s *pool.Session,
	queueName string,
	consumerName string,
	messageGenerator func() string,
	n int,
) {
	delivery, err := s.Consume(
		queueName,
		pool.ConsumeOptions{
			ConsumerTag: consumerName,
			Exclusive:   true,
		},
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}

	wg.Add(1)
	go func(wg *sync.WaitGroup, messageGenerator func() string, n int) {
		defer wg.Done()
		cctx, ccancel := context.WithCancel(ctx)
		defer ccancel()

		msgsReceived := 0
		defer func() {
			assert.Equal(t, n, msgsReceived)
		}()
		for {
			select {
			case <-cctx.Done():
				return
			case val, ok := <-delivery:
				if !ok {
					return
				}
				err := val.Ack(false)
				if err != nil {
					assert.NoError(t, err)
					return
				}

				receivedMsg := string(val.Body)
				assert.Equal(t, messageGenerator(), receivedMsg)
				msgsReceived++
				if msgsReceived == n {
					ccancel()
				}
			}
		}
	}(wg, messageGenerator, n)
}

func PublishAsyncN(
	t *testing.T,
	ctx context.Context,
	wg *sync.WaitGroup,
	s *pool.Session,
	exchangeName string,
	publishMessageGenerator func() string,
	n int,
) {
	wg.Add(1)
	go func(wg *sync.WaitGroup, publishMessageGenerator func() string, n int) {
		defer wg.Done()

		for i := 0; i < n; i++ {
			func() {
				tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()

				tag, err := s.Publish(
					tctx,
					exchangeName, "",
					pool.Publishing{
						Mandatory:   true,
						ContentType: "text/plain",
						Body:        []byte(publishMessageGenerator()),
					})
				if err != nil {
					assert.NoError(t, err)
					return
				}
				if s.IsConfirmable() {
					err = s.AwaitConfirm(tctx, tag)
					if err != nil {
						assert.NoError(t, err)
						return
					}
				}
			}()
		}
	}(wg, publishMessageGenerator, n)
}

func DeclareExchangeQueue(
	t *testing.T,
	ctx context.Context,
	s *pool.Session,
	exchangeName string,
	queueName string,
) (cleanup func()) {
	cleanup = func() {}
	var err error

	err = s.ExchangeDeclare(ctx, exchangeName, pool.ExchangeKindTopic)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer func() {
		if err != nil {
			assert.NoError(t, s.ExchangeDelete(ctx, exchangeName))
		}
	}()

	_, err = s.QueueDeclare(ctx, queueName)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer func() {
		if err != nil {
			_, e := s.QueueDelete(ctx, queueName)
			assert.NoError(t, e)
		}
	}()

	err = s.QueueBind(ctx, queueName, "#", exchangeName)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer func() {
		if err != nil {
			assert.NoError(t, s.QueueUnbind(ctx, queueName, "#", exchangeName, nil))
		}
	}()

	return func() {
		assert.NoError(t, s.QueueUnbind(ctx, queueName, "#", exchangeName, nil))

		_, e := s.QueueDelete(ctx, queueName)
		assert.NoError(t, e)
		assert.NoError(t, s.ExchangeDelete(ctx, exchangeName))
	}
}
