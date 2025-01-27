package pool

import (
	"context"

	"github.com/jxsl13/amqpx/logging"
	"github.com/jxsl13/amqpx/types"
)

type publisherOption struct {
	Ctx context.Context

	AutoClosePool bool
	BackoffPolicy types.BackoffFunc

	Logger logging.Logger
}

type PublisherOption func(*publisherOption)

func PublisherWithContext(ctx context.Context) PublisherOption {
	return func(po *publisherOption) {
		if ctx != nil {
			po.Ctx = ctx
		}
	}
}

func PublisherWithLogger(logger logging.Logger) PublisherOption {
	return func(po *publisherOption) {
		if logger != nil {
			po.Logger = logger
		}
	}
}

func PublisherWithAutoClosePool(autoClose bool) PublisherOption {
	return func(po *publisherOption) {
		po.AutoClosePool = autoClose
	}
}

func PublisherWithBackoffPolicy(backoffFunc types.BackoffFunc) PublisherOption {
	return func(po *publisherOption) {
		if backoffFunc != nil {
			po.BackoffPolicy = backoffFunc
		}
	}
}
