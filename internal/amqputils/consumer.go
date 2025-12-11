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

	if batchSize <= 0 {
		panic("batchSize must be greater than 0")
	}

	if batchCount <= 0 {
		panic("batchCount must be greater than 0")
	}

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

	if batchSize <= 0 {
		panic("batchSize must be greater than 0")
	}

	if batchCount <= 0 {
		panic("batchCount must be greater than 0")
	}

	cctx, ccancel := context.WithCancel(ctx)
	defer ccancel()
	log := testlogger.NewTestLogger(t)

	currentBatch := 1
	// initial batch must be defined before any loop iteration, neither outer nor inner loop.
	batch := make([]string, 0, batchSize)
	for range batchSize {
		batch = append(batch, messageGenerator())
	}

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
		if !assert.NoError(t, err) {
			// continue on error
			continue
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
				if !assert.NoError(t, err) {
					continue outer
				}

				receivedMsg := string(val.Body)
				expectedMsg := batch[i]
				firstBatchMsg := batch[0]

				if allowDuplicates && i > 0 && firstBatchMsg == receivedMsg {
					// INFO: it is possible that messages are duplicated, but this is not a problem, we allow that
					// due to network issues. We should not fail the test in this case.
					log.Warn(fmt.Sprintf("detected duplicate batch start message, found %d duplicate messages", i))
					i = 0
					expectedMsg = firstBatchMsg
				}
				i++

				assert.Equalf(
					t,
					expectedMsg,
					receivedMsg,
					"expected message %s, got %s",
					expectedMsg,
					receivedMsg,
				)

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
					i = 0
					currentBatch++
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
