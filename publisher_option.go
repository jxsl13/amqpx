package amqpx

import "context"

type publisherOption struct {
	Ctx     context.Context
	Ackable bool
}

type PublisherOption func(*publisherOption)

func PublisherWithContext(ctx context.Context) PublisherOption {
	return func(po *publisherOption) {
		po.Ctx = ctx
	}
}

func PublisherWithAckable(ackable bool) PublisherOption {
	return func(po *publisherOption) {
		po.Ackable = ackable
	}
}
