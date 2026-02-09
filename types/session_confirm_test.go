package types_test

import (
	"context"
	"testing"
	"time"

	"github.com/jxsl13/amqpx/internal/amqputils"
	"github.com/jxsl13/amqpx/internal/testlogger"
	"github.com/jxsl13/amqpx/internal/testutils"
	"github.com/jxsl13/amqpx/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAwaitConfirmStaleDeliveryTag reproduces the production bug where
// a context cancellation during AwaitConfirm leaves a stale confirm in
// the channel, causing all subsequent AwaitConfirm calls to fail with
// "delivery tag mismatch: expected N+1, got N" perpetually.
func TestAwaitConfirmStaleDeliveryTag(t *testing.T) {
	t.Parallel()
	var (
		ctx              = context.TODO()
		nextConnName     = testutils.ConnectionNameGenerator()
		connName         = nextConnName()
		nextSessionName  = testutils.SessionNameGenerator(connName)
		sessionName      = nextSessionName()
		nextQueueName    = testutils.QueueNameGenerator(sessionName)
		queueName        = nextQueueName()
		nextExchangeName = testutils.ExchangeNameGenerator(sessionName)
		exchangeName     = nextExchangeName()
	)

	c, err := types.NewConnection(
		ctx,
		testutils.HealthyConnectURL,
		connName,
		types.ConnectionWithLogger(testlogger.NewTestLogger(t)),
	)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, c.Close())
	}()

	s, err := types.NewSession(
		c,
		sessionName,
		types.SessionWithConfirms(true),
		types.SessionWithRetryCallback(
			func(operation, connName, sessionName string, retry int, err error) {
				t.Logf("operation=%s connName=%s sessionName=%s retry=%d err=%v", operation, connName, sessionName, retry, err)
			},
		),
	)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, s.Close())
	}()

	cleanup := amqputils.DeclareExchangeQueue(t, ctx, s, exchangeName, queueName)
	defer cleanup()

	// Step 1: Publish message 1 and get tag 1
	tag1, err := s.Publish(ctx, exchangeName, "", types.Publishing{
		ContentType: "text/plain",
		Body:        []byte("message-1"),
	})
	require.NoError(t, err)
	t.Logf("Published message 1 with tag %d", tag1)

	// Step 2: Simulate context cancellation during AwaitConfirm
	// Use an already-cancelled context so AwaitConfirm returns immediately
	// without consuming the confirm for tag 1.
	cancelledCtx, cancel := context.WithCancel(ctx)
	cancel() // cancel immediately

	err = s.AwaitConfirm(cancelledCtx, tag1)
	require.Error(t, err, "expected error from cancelled context")
	t.Logf("AwaitConfirm with cancelled context returned: %v", err)

	// Give the broker a moment to send the confirm for tag 1
	time.Sleep(100 * time.Millisecond)

	// Step 3: Publish message 2 and get tag 2
	tag2, err := s.Publish(ctx, exchangeName, "", types.Publishing{
		ContentType: "text/plain",
		Body:        []byte("message-2"),
	})
	require.NoError(t, err)
	t.Logf("Published message 2 with tag %d", tag2)
	require.Equal(t, tag1+1, tag2, "tag2 should be tag1+1")

	// Step 4: AwaitConfirm for tag 2 should succeed
	// BUG: Without the fix, this reads the stale confirm for tag 1
	// and returns "delivery tag mismatch: expected 2, got 1"
	err = s.AwaitConfirm(ctx, tag2)
	assert.NoError(t, err, "AwaitConfirm for tag2 should succeed by skipping stale confirm for tag1")
}

// TestAwaitConfirmMultipleStaleDeliveryTags tests that multiple stale confirms
// are properly skipped when several publishes had their AwaitConfirm interrupted.
func TestAwaitConfirmMultipleStaleDeliveryTags(t *testing.T) {
	t.Parallel()
	var (
		ctx              = context.TODO()
		nextConnName     = testutils.ConnectionNameGenerator()
		connName         = nextConnName()
		nextSessionName  = testutils.SessionNameGenerator(connName)
		sessionName      = nextSessionName()
		nextQueueName    = testutils.QueueNameGenerator(sessionName)
		queueName        = nextQueueName()
		nextExchangeName = testutils.ExchangeNameGenerator(sessionName)
		exchangeName     = nextExchangeName()
	)

	c, err := types.NewConnection(
		ctx,
		testutils.HealthyConnectURL,
		connName,
		types.ConnectionWithLogger(testlogger.NewTestLogger(t)),
	)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, c.Close())
	}()

	s, err := types.NewSession(
		c,
		sessionName,
		types.SessionWithConfirms(true),
	)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, s.Close())
	}()

	cleanup := amqputils.DeclareExchangeQueue(t, ctx, s, exchangeName, queueName)
	defer cleanup()

	cancelledCtx, cancel := context.WithCancel(ctx)
	cancel()

	// Publish 5 messages, only await confirm on the last one
	var lastTag uint64
	for i := 0; i < 5; i++ {
		tag, err := s.Publish(ctx, exchangeName, "", types.Publishing{
			ContentType: "text/plain",
			Body:        []byte("message"),
		})
		require.NoError(t, err)

		if i < 4 {
			// Simulate context cancellation - don't consume the confirm
			err = s.AwaitConfirm(cancelledCtx, tag)
			require.Error(t, err)
		} else {
			lastTag = tag
		}
	}

	// Give broker time to send all confirms
	time.Sleep(200 * time.Millisecond)

	// AwaitConfirm on the last tag should skip all 4 stale confirms
	err = s.AwaitConfirm(ctx, lastTag)
	assert.NoError(t, err, "should skip stale confirms and match the last tag")
}

// TestAwaitConfirmConcurrentPublishersWithContextTimeout simulates the production
// scenario where many concurrent publishers share a session pool, and context
// timeouts cause delivery tag drift.
func TestAwaitConfirmConcurrentPublishersWithContextTimeout(t *testing.T) {
	t.Parallel()
	var (
		ctx              = context.TODO()
		nextConnName     = testutils.ConnectionNameGenerator()
		connName         = nextConnName()
		nextSessionName  = testutils.SessionNameGenerator(connName)
		sessionName      = nextSessionName()
		nextQueueName    = testutils.QueueNameGenerator(sessionName)
		queueName        = nextQueueName()
		nextExchangeName = testutils.ExchangeNameGenerator(sessionName)
		exchangeName     = nextExchangeName()
		numPublishers    = 10
	)

	c, err := types.NewConnection(
		ctx,
		testutils.HealthyConnectURL,
		connName,
		types.ConnectionWithLogger(testlogger.NewTestLogger(t)),
	)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, c.Close())
	}()

	s, err := types.NewSession(
		c,
		sessionName,
		types.SessionWithConfirms(true),
	)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, s.Close())
	}()

	cleanup := amqputils.DeclareExchangeQueue(t, ctx, s, exchangeName, queueName)
	defer cleanup()

	// Simulate: a cancelled publish followed by a normal publish, sequentially
	// (since sessions must be used by one goroutine at a time)
	cancelledCtx, cancel := context.WithCancel(ctx)
	cancel()

	// First: cancelled publish leaves stale confirm
	tag1, err := s.Publish(ctx, exchangeName, "", types.Publishing{
		ContentType: "text/plain",
		Body:        []byte("cancelled-publish"),
	})
	require.NoError(t, err)

	err = s.AwaitConfirm(cancelledCtx, tag1)
	require.Error(t, err) // context cancelled

	time.Sleep(100 * time.Millisecond)

	// Now: many sequential publishes should all succeed despite the initial stale confirm
	for i := 0; i < numPublishers; i++ {
		tag, err := s.Publish(ctx, exchangeName, "", types.Publishing{
			ContentType: "text/plain",
			Body:        []byte("normal-publish"),
		})
		require.NoError(t, err)

		err = s.AwaitConfirm(ctx, tag)
		assert.NoError(t, err, "publish %d: AwaitConfirm should succeed", i)
		if err != nil {
			t.Fatalf("publish %d failed: %v", i, err)
		}
	}
}
