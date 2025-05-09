package amqputils

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/jxsl13/amqpx/internal/testlogger"
	"github.com/jxsl13/amqpx/types"
	"github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
)

type Consumer interface {
	ConsumeWithContext(ctx context.Context, queue string, option ...types.ConsumeOptions) (<-chan amqp091.Delivery, error)
}

func ConsumeN(
	t *testing.T,
	ctx context.Context,
	c Consumer,
	queueName string,
	consumerName string,
	messageGenerator func() string,
	n int,
	allowDuplicates bool,
) {
	cctx, ccancel := context.WithCancel(ctx)
	defer ccancel()
	log := testlogger.NewTestLogger(t)

	msgsReceived := 0
	defer func() {
		assert.Equal(t, n, msgsReceived, "expected to consume %d messages, got %d", n, msgsReceived)
	}()

	var previouslyReceivedMsg string

outer:
	for {
		delivery, err := c.ConsumeWithContext(
			ctx,
			queueName,
			types.ConsumeOptions{
				ConsumerTag: consumerName,
				Exclusive:   true,
			},
		)
		if err != nil {
			assert.NoError(t, err)
			return
		}

		for {
			select {
			case <-cctx.Done():
				return
			case val, ok := <-delivery:
				if !ok {
					continue outer
				}
				err := val.Ack(false)
				if err != nil {
					assert.NoError(t, err)
					return
				}

				var receivedMsg = string(val.Body)
				if allowDuplicates && receivedMsg == previouslyReceivedMsg {
					// INFO: it is possible that messages are duplicated, but this is not a problem, we allow that
					// due to network issues. We should not fail the test in this case.
					log.Warn(fmt.Sprintf("received duplicate message: %s", receivedMsg))
					continue
				}

				var expectedMsg = messageGenerator()
				assert.Equalf(
					t,
					expectedMsg,
					receivedMsg,
					"expected message %s, got %s, previously received message: %s",
					expectedMsg,
					receivedMsg,
					previouslyReceivedMsg,
				)

				log.Info(fmt.Sprintf("consumed message: %s", receivedMsg))
				msgsReceived++
				if msgsReceived == n {
					log.Info(fmt.Sprintf("consumed %d messages, closing consumer", n))
					ccancel()
				}
				// update last received message
				previouslyReceivedMsg = receivedMsg
			}
		}
	}
}

func ConsumeAsyncN(
	t *testing.T,
	ctx context.Context,
	wg *sync.WaitGroup,
	c Consumer,
	queueName string,
	consumerName string,
	messageGenerator func() string,
	n int,
	alllowDuplicates bool,
) {

	wg.Add(1)
	go func() {
		defer wg.Done()
		ConsumeN(t, ctx, c, queueName, consumerName, messageGenerator, n, alllowDuplicates)
	}()
}
