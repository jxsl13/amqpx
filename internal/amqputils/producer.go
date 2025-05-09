package amqputils

import (
	"context"
	"fmt"
	"sync"
	"testing"

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
	tag, err := p.Publish(
		ctx,
		exchangeName, "",
		types.Publishing{
			Mandatory:   true,
			ContentType: "text/plain",
			Body:        []byte(message),
		})
	if err != nil {
		return fmt.Errorf("expected no error when publishing message: %w", err)
	}
	if p.IsConfirmable() {
		err = p.AwaitConfirm(ctx, tag)
		if err != nil {
			return fmt.Errorf("expected no error when awaiting confirmation: %w", err)
		}
	}
	return nil
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
