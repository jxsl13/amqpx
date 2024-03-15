package pool

import (
	"context"

	"github.com/jxsl13/amqpx/logging"
)

type publisherOption struct {
	Ctx context.Context

	AutoClosePool bool

	Logger logging.Logger
}

type PublisherOption func(*publisherOption)

func PublisherWithContext(ctx context.Context) PublisherOption {
	return func(po *publisherOption) {
		po.Ctx = ctx
	}
}

func PublisherWithLogger(logger logging.Logger) PublisherOption {
	return func(po *publisherOption) {
		po.Logger = logger
	}
}

func PublisherWithAutoClosePool(autoClose bool) PublisherOption {
	return func(po *publisherOption) {
		po.AutoClosePool = autoClose
	}
}
