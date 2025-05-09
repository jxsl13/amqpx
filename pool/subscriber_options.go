package pool

import (
	"context"
	"log/slog"
)

type subscriberOption struct {
	Ctx           context.Context
	AutoClosePool bool

	Logger *slog.Logger
}

type SubscriberOption func(*subscriberOption)

func SubscriberWithContext(ctx context.Context) SubscriberOption {
	return func(co *subscriberOption) {
		co.Ctx = ctx
	}
}

func SubscriberWithLogger(logger *slog.Logger) SubscriberOption {
	return func(co *subscriberOption) {
		co.Logger = logger
	}
}

func SubscriberWithAutoClosePool(autoClose bool) SubscriberOption {
	return func(co *subscriberOption) {
		co.AutoClosePool = autoClose
	}
}
