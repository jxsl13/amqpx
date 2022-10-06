package pool

import (
	"context"
	"time"

	"github.com/jxsl13/amqpx/logging"
)

type publisherOption struct {
	Ctx            context.Context
	PublishTimeout time.Duration
	ConfirmTimeout time.Duration
	AutoClosePool  bool

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

func PublisherWithConfirmTimeout(timeout time.Duration) PublisherOption {
	if timeout < time.Second {
		timeout = time.Second
	}
	return func(po *publisherOption) {
		po.ConfirmTimeout = timeout
	}
}

func PublisherWithPublishTimeout(timeout time.Duration) PublisherOption {
	if timeout < time.Second {
		timeout = time.Second
	}
	return func(po *publisherOption) {
		po.PublishTimeout = timeout
	}
}
