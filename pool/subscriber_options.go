package pool

import (
	"context"
	"time"

	"github.com/jxsl13/amqpx/logging"
)

type subscriberOption struct {
	Ctx           context.Context
	AutoClosePool bool

	CloseTimeout time.Duration

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

func SubscriberWitCloseTimeout(timeout time.Duration) SubscriberOption {
	return func(co *subscriberOption) {
		if timeout <= 0 {
			co.CloseTimeout = 5 * time.Second
		} else {
			co.CloseTimeout = timeout
		}
	}
}
