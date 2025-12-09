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

func ConsumeBatchAsyncN(
	t *testing.T,
	ctx context.Context,
	wg *sync.WaitGroup,
	c Consumer,
	queueName string,
	consumerName string,
	messageGenerator func() string,
	batchSize int,
	batchCount int,
	alllowDuplicates bool,
) {

	wg.Add(1)
	go func() {
		defer wg.Done()
		ConsumeBatchN(t, ctx, c, queueName, consumerName, messageGenerator, batchSize, batchCount, alllowDuplicates)
	}()
}

func ConsumeBatchN(
	t *testing.T,
	ctx context.Context,
	c Consumer,
	queueName string,
	consumerName string,
	messageGenerator func() string,
	batchSize int,
	batchCount int,
	allowDuplicates bool,
) {
	cctx, ccancel := context.WithCancel(ctx)
	defer ccancel()
	log := testlogger.NewTestLogger(t)

	currentBatch := 1
outer:
	for {
		delivery, err := c.ConsumeWithContext(
			cctx,
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

		batch := make([]string, 0, batchSize)
		for range batchSize {
			batch = append(batch, messageGenerator())
		}

		i := 0

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

				receivedMsg := string(val.Body)

				if allowDuplicates && i > 0 && batch[0] == receivedMsg {
					// INFO: it is possible that messages are duplicated, but this is not a problem, we allow that
					// due to network issues. We should not fail the test in this case.
					log.Warn(fmt.Sprintf("detected duplicate batch start message, found %d duplicate messages", i))
					i = 0
				}
				expectedMsg := batch[i]

				assert.Equalf(
					t,
					expectedMsg,
					receivedMsg,
					"expected message %s, got %s, previously received message: %s",
					expectedMsg,
					receivedMsg,
					batch[max(i-1, 0)],
				)

				i++

				log.Info(fmt.Sprintf("consumed message: %s", receivedMsg))
				if i == batchSize {

					log.Info(fmt.Sprintf("consumed %d messages in batch %d/%d", batchSize, currentBatch, batchCount))
					if currentBatch == batchCount {
						ccancel()
						continue
					}

					// prepare for next batch
					batch = batch[:0]
					for range batchSize {
						batch = append(batch, messageGenerator())
					}
					currentBatch++
					i = 0
				}
			}
		}
	}
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

				receivedMsg := string(val.Body)
				if allowDuplicates && receivedMsg == previouslyReceivedMsg {
					// INFO: it is possible that messages are duplicated, but this is not a problem, we allow that
					// due to network issues. We should not fail the test in this case.
					log.Warn(fmt.Sprintf("received duplicate message: %s", receivedMsg))
					continue
				}

				expectedMsg := messageGenerator()
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
