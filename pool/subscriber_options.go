package pool

import "context"

type subscriberOption struct {
	Ctx           context.Context
	AutoClosePool bool
}

type SubscriberOption func(*subscriberOption)

func SubscriberWithContext(ctx context.Context) SubscriberOption {
	return func(co *subscriberOption) {
		co.Ctx = ctx
	}
}

func SubscriberWithAutoClosePool(autoClose bool) SubscriberOption {
	return func(co *subscriberOption) {
		co.AutoClosePool = autoClose
	}
}
