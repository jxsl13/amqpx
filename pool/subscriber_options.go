package pool

import (
	"context"

	"github.com/jxsl13/amqpx/logging"
)

type subscriberOption struct {
	Ctx           context.Context
	AutoClosePool bool

	Logger logging.Logger
}

type SubscriberOption func(*subscriberOption)

func SubscriberWithContext(ctx context.Context) SubscriberOption {
	return func(co *subscriberOption) {
		co.Ctx = ctx
	}
}

func SubscriberWithLogger(logger logging.Logger) SubscriberOption {
	return func(co *subscriberOption) {
		co.Logger = logger
	}
}

func SubscriberWithAutoClosePool(autoClose bool) SubscriberOption {
	return func(co *subscriberOption) {
		co.AutoClosePool = autoClose
	}
}
