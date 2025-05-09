package amqputils

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jxsl13/amqpx/internal/testlogger"
	"github.com/jxsl13/amqpx/types"
	"github.com/stretchr/testify/assert"
)

type Producer interface {
	Publish(ctx context.Context, exchange string, routingKey string, msg types.Publishing) (deliveryTag uint64, err error)
	IsConfirmable() bool
	AwaitConfirm(ctx context.Context, expectedTag uint64) error
}

func PublishN(
	t *testing.T,
	ctx context.Context,
	p Producer,
	exchangeName string,
	publishMessageGenerator func() string,
	n int,
) {
	for i := 0; i < n; i++ {
		message := publishMessageGenerator()
		err := publish(ctx, p, exchangeName, message)
		assert.NoError(t, err)
	}
	testlogger.NewTestLogger(t).Info(fmt.Sprintf("published %d messages, closing publisher", n))
}

func publish(ctx context.Context, p Producer, exchangeName string, message string) error {

	return retry(ctx, func() error {
		tag, err := p.Publish(
			ctx,
			exchangeName, "",
			types.Publishing{
				Mandatory:   true,
				ContentType: "text/plain",
				Body:        []byte(message),
			})
		if err != nil {
			return fmt.Errorf("failed to publish message: %w", err)
		}
		if !p.IsConfirmable() {
			return nil
		}

		err = p.AwaitConfirm(ctx, tag)
		if err != nil {
			return fmt.Errorf("failed to await message confirmation: %w", err)
		}

		return nil
	})
}

func retry(ctx context.Context, f func() error) error {
	err := f()
	if err == nil {
		return nil
	}
	retry := 0
	backoff := types.NewBackoffPolicy(100*time.Millisecond, 5*time.Second)
	for {
		retry++
		select {
		case <-ctx.Done():
			return errors.Join(err, ctx.Err())
		case <-time.After(backoff(retry)):
			err = f()
			if err != nil {
				continue
			}
			return nil
		}
	}
}

func PublishAsyncN(
	t *testing.T,
	ctx context.Context,
	wg *sync.WaitGroup,
	p Producer,
	exchangeName string,
	publishMessageGenerator func() string,
	n int,
) {
	wg.Add(1)
	go func(wg *sync.WaitGroup, publishMessageGenerator func() string, n int) {
		defer wg.Done()
		PublishN(t, ctx, p, exchangeName, publishMessageGenerator, n)
	}(wg, publishMessageGenerator, n)
}
