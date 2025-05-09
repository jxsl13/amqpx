package amqputils

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jxsl13/amqpx/internal/testlogger"
	"github.com/jxsl13/amqpx/internal/testutils"
	"github.com/jxsl13/amqpx/pool"
	"github.com/jxsl13/amqpx/types"
	"github.com/stretchr/testify/assert"
)

func SubscriberConsumeN(
	t *testing.T,
	ctx context.Context,
	p *pool.Pool,
	queueName string,
	consumerName string,
	messageGenerator func() string,
	n int,
	allowDuplicates bool,
) {
	var log = testlogger.NewTestLogger(t)

	processingTime := 30 * time.Millisecond
	cctx, ccancel := context.WithTimeout(ctx, 30*time.Second+time.Duration((2+1)*n)*processingTime)
	defer ccancel()
	sub := pool.NewSubscriber(
		p,
		pool.SubscriberWithContext(cctx),
		pool.SubscriberWithLogger(log),
	)
	defer sub.Close()

	msgsReceived := 0
	defer func() {
		assert.Equal(t, n, msgsReceived, "expected to consume %d messages, got %d", n, msgsReceived)
	}()

	var previouslyReceivedMsg string
	sub.RegisterHandlerFunc(queueName, func(ctx context.Context, d types.Delivery) error {
		var receivedMsg = string(d.Body)

		select {
		case <-time.After(testutils.Jitter(0, processingTime)):
		case <-ctx.Done():
		}

		if allowDuplicates && receivedMsg == previouslyReceivedMsg {
			// INFO: it is possible that messages are duplicated, but this is not a problem
			// due to network issues. We should not fail the test in this case.
			log.Warn(fmt.Sprintf("received duplicate message: %s", receivedMsg))
			return nil
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
		return nil
	}, types.ConsumeOptions{
		ConsumerTag: consumerName,
	})

	err := sub.Start(cctx)
	if err != nil {
		assert.NoError(t, err)
		ccancel()
	}
	sub.Wait()
}

func SubscriberConsumeAsyncN(
	t *testing.T,
	ctx context.Context,
	wg *sync.WaitGroup,
	p *pool.Pool,
	queueName string,
	consumerName string,
	messageGenerator func() string,
	n int,
	allowDuplicates bool,
) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		SubscriberConsumeN(t, ctx, p, queueName, consumerName, messageGenerator, n, allowDuplicates)
	}()
}

func SubscriberBatchConsumeN(
	t *testing.T,
	ctx context.Context,
	p *pool.Pool,
	queueName string,
	consumerName string,
	messageGenerator func() string,
	n int,
	batchSize int,
	maxBytes int,
	allowDuplicates bool,
) {
	var log = testlogger.NewTestLogger(t)
	processingTime := 30 * time.Millisecond
	cctx, ccancel := context.WithTimeout(ctx, 30*time.Second+time.Duration((2+1)*n)*processingTime)
	defer ccancel()
	sub := pool.NewSubscriber(
		p,
		pool.SubscriberWithContext(cctx),
		pool.SubscriberWithLogger(log),
	)
	defer sub.Close()

	msgsReceived := 0
	defer func() {
		assert.Equal(t, n, msgsReceived, "expected to consume %d messages, got %d", n, msgsReceived)
	}()

	var previouslyReceivedMsg string
	sub.RegisterBatchHandlerFunc(queueName, func(ctx context.Context, ds []types.Delivery) error {
		for _, d := range ds {
			var receivedMsg = string(d.Body)

			select {
			case <-time.After(testutils.Jitter(0, processingTime)):
			case <-ctx.Done():
			}

			if allowDuplicates && receivedMsg == previouslyReceivedMsg {
				// INFO: it is possible that messages are duplicated, but this is not a problem
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
		return nil
	}, pool.WithBatchConsumeOptions(types.ConsumeOptions{
		ConsumerTag: consumerName,
	}), pool.WithMaxBatchSize(batchSize),
		pool.WithMaxBatchBytes(maxBytes),
	)

	err := sub.Start(cctx)
	if err != nil {
		assert.NoError(t, err)
		ccancel()
	}
	sub.Wait()
}

func SubscriberBatchConsumeAsyncN(
	t *testing.T,
	ctx context.Context,
	wg *sync.WaitGroup,
	p *pool.Pool,
	queueName string,
	consumerName string,
	messageGenerator func() string,
	n int,
	batchSize int,
	maxBytes int,
	allowDuplicates bool,
) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		SubscriberBatchConsumeN(
			t,
			ctx,
			p,
			queueName,
			consumerName,
			messageGenerator,
			n,
			batchSize,
			maxBytes,
			allowDuplicates,
		)
	}()
}
