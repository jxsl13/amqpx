package pool

import (
	"context"
	"time"
)

type publisherOption struct {
	Ctx            context.Context
	PublishTimeout time.Duration
	ConfirmTimeout time.Duration
	AutoClose      bool
}

type PublisherOption func(*publisherOption)

func PublisherWithContext(ctx context.Context) PublisherOption {
	return func(po *publisherOption) {
		po.Ctx = ctx
	}
}

func PublisherWithAutoClosePool(autoClose bool) PublisherOption {
	return func(po *publisherOption) {
		po.AutoClose = autoClose
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
