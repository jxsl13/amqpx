package pool_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jxsl13/amqpx/internal/testutils"
	"github.com/jxsl13/amqpx/logging"
	"github.com/jxsl13/amqpx/pool"
	"github.com/stretchr/testify/assert"
)

func TestSinglePublisher(t *testing.T) {
	t.Parallel()

	var (
		proxyName, connectURL, _ = testutils.NextConnectURL()
		ctx                      = context.TODO()
		log                      = logging.NewTestLogger(t)
		nextConnName             = testutils.ConnectionNameGenerator()
		numMsgs                  = 5
	)

	hs, hsclose := NewSession(
		t,
		ctx,
		testutils.HealthyConnectURL,
		nextConnName(),
	)
	defer hsclose()

	p, err := pool.New(
		ctx,
		connectURL,
		1,
		1,
		pool.WithLogger(logging.NewTestLogger(t)),
		pool.WithConfirms(true),
		pool.WithConnectionRecoverCallback(func(name string, retry int, err error) {
			log.Warnf("connection %s is broken, retry %d, error: %s", name, retry, err)
		}),
	)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer p.Close()

	var (
		nextExchangeName = testutils.ExchangeNameGenerator(hs.Name())
		nextQueueName    = testutils.QueueNameGenerator(hs.Name())
		exchangeName     = nextExchangeName()
		queueName        = nextQueueName()
	)
	cleanup := DeclareExchangeQueue(t, ctx, hs, exchangeName, queueName)
	defer cleanup()

	var (
		nextConsumerName = testutils.ConsumerNameGenerator(queueName)
		publisherMsgGen  = testutils.MessageGenerator(queueName)
		consumerMsgGen   = testutils.MessageGenerator(queueName)
		wg               sync.WaitGroup
	)

	pub := pool.NewPublisher(p)
	defer pub.Close()

	// TODO: currently this test allows duplication of messages
	ConsumeAsyncN(t, ctx, &wg, hs, queueName, nextConsumerName(), consumerMsgGen, numMsgs, true)

	for i := 0; i < numMsgs; i++ {
		msg := publisherMsgGen()
		err = func(msg string) error {
			disconnected, reconnected := DisconnectWithStartedStopped(
				t,
				proxyName,
				0,
				testutils.Jitter(time.Millisecond, 20*time.Millisecond),
				testutils.Jitter(100*time.Millisecond, 150*time.Millisecond),
			)
			defer func() {
				disconnected()
				reconnected()
			}()

			return pub.Publish(ctx, exchangeName, "", pool.Publishing{
				Mandatory:   true,
				ContentType: "text/plain",
				Body:        []byte(msg),
			})
		}(msg)
		if err != nil {
			assert.NoError(t, err, fmt.Sprintf("when publishing message %s", msg))
			return
		}
	}
	wg.Wait()
}

/*
// TODO: out of memory rabbitmq tests are disabled until https://github.com/rabbitmq/amqp091-go/issues/253 is resolved
func TestPublishAwaitFlowControl(t *testing.T) {
	t.Parallel()

	var (
		ctx          = context.TODO()
		nextConnName = testutils.ConnectionNameGenerator()
	)

	hs, hsclose := NewSession(
		t,
		ctx,
		testutils.HealthyConnectURL,
		nextConnName(),
	)
	defer hsclose()

	p, err := pool.New(
		ctx,
		testutils.BrokenConnectURL,
		1,
		1,
		pool.WithLogger(logging.NewTestLogger(t)),
		pool.WithConfirms(true),
	)
	if !assert.NoError(t, err) {
		return
	}
	defer func() {
		p.Close()
	}()
	var (
		nextExchangeName = testutils.ExchangeNameGenerator(hs.Name())
		nextQueueName    = testutils.QueueNameGenerator(hs.Name())
		exchangeName     = nextExchangeName()
		queueName        = nextQueueName()
	)
	ts, err := p.GetTransientSession(ctx)
	if !assert.NoError(t, err) {
		return
	}
	defer func() {
		p.ReturnSession(ts, nil)
	}()
	cleanup := DeclareExchangeQueue(t, ctx, ts, exchangeName, queueName)
	defer cleanup()

	var (
		publisherMsgGen = testutils.MessageGenerator(queueName)
	)
	pub := pool.NewPublisher(p)
	defer pub.Close()

	tctx, tcancel := context.WithTimeout(ctx, 10*time.Second)
	defer tcancel()

	err = pub.Publish(tctx, exchangeName, "", pool.Publishing{
		ContentType: "text/plain",
		Body:        []byte(publisherMsgGen()),
	})
	if !assert.ErrorIs(t, err, context.DeadlineExceeded) {
		t.Fatal("expected context deadline exceeded error")
	}
	// FIXME: this test gets stuck when the sessions in the session pool are closed.:
}
*/
