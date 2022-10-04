package amqpx

import (
	"context"

	"github.com/jxsl13/amqpx/pool"
)

type Publisher struct {
	pool    *pool.Pool
	ackable bool

	message chan pool.Publishing // unbuffered channel, worker must explicitly fetch message
	pending chan pool.Publishing // buffered with a single element for retrying

	ctx    context.Context
	cancel context.CancelFunc
}

func NewPublisher(p *pool.Pool, options ...PublisherOption) (*Publisher, error) {
	if p == nil {
		panic("nil pool passed")
	}

	option := publisherOption{
		Ctx:     p.Context(),
		Ackable: true,
	}

	for _, o := range options {
		o(&option)
	}

	ctx, cancel := context.WithCancel(option.Ctx)

	pub := &Publisher{
		pool:    p,
		ackable: option.Ackable,
		message: make(chan pool.Publishing),
		pending: make(chan pool.Publishing, 1),

		ctx:    ctx,
		cancel: cancel,
	}

	// TODO: continue here

	return pub, nil
}
