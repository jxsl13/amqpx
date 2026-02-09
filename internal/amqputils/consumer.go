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

	numBatches := 0
	// initial batch must be defined before any loop iteration, neither outer nor inner loop.
	expectedBatch := make([]string, 0, batchSize)
	for range batchSize {
		expectedBatch = append(expectedBatch, messageGenerator())
	}

	// track previous batch messages for cross-batch duplicate detection
	var previousBatchSet map[string]struct{}

	i := 0
	currentBatch := make([]string, 0, batchSize)

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
			// continue on error
			continue outer
		}

		for {
			select {
			case <-cctx.Done():
				return
			case val, ok := <-delivery:
				if !ok {
					return
				}

				err := val.Ack(false)
				if !assert.NoError(t, err) {
					return
				}

				// it is possible that after the complete batch was sent, we resend
				// the complete batch because of network problems that kill the channel before the confirmations
				// are awaited. In that case we reset the batch at the beginning of the batchSize +1's message.
				if len(currentBatch) == batchSize {
					// only the last batch may leave messages in the queue, because we cannot otherwise stop consuming
					// in the normal case, where the whole batch is not resent.
					i = 0
					currentBatch = currentBatch[:0]
				}

				receivedMsg := string(val.Body)

				// skip duplicates from the previous batch (cross-batch retry)
				if allowDuplicates && len(previousBatchSet) > 0 {
					if _, isDup := previousBatchSet[receivedMsg]; isDup {
						log.Warn(fmt.Sprintf("skipping duplicate from previous batch: %s", receivedMsg))
						continue
					}
				}

				expectedMsg := expectedBatch[i%len(expectedBatch)]
				if allowDuplicates && i > 0 && receivedMsg == expectedBatch[0] {
					i = 0
					expectedMsg = expectedBatch[0]
					currentBatch = currentBatch[:0]
					log.Warn(fmt.Sprintf("received duplicate message: %s", receivedMsg))
				}

				// append to either reset and empty or partial batch
				currentBatch = append(currentBatch, receivedMsg)
				i++

				if !assert.Equalf(
					t,
					expectedMsg,
					receivedMsg,
					"expected message %s, got %s",
					expectedMsg,
					receivedMsg,
				) {
					log.Warn(fmt.Sprintf("message mismatch expected %q, got %q", expectedMsg, receivedMsg))
				}

				log.Info(fmt.Sprintf("consumed message: %s", receivedMsg))
				if len(currentBatch) == batchSize {
					assert.Equal(t,
						expectedBatch,
						currentBatch,
						"expected batch %v, got %v",
						expectedBatch,
						currentBatch,
					)
					numBatches++
					log.Info(fmt.Sprintf("consumed %d messages in batch %d/%d", batchSize, numBatches, batchCount))
					if numBatches >= batchCount {
						ccancel()
						continue
					}

					// advance to next batch: save current as previous, generate new expected
					previousBatchSet = make(map[string]struct{}, batchSize)
					for _, msg := range expectedBatch {
						previousBatchSet[msg] = struct{}{}
					}
					expectedBatch = expectedBatch[:0]
					for range batchSize {
						expectedBatch = append(expectedBatch, messageGenerator())
					}
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
