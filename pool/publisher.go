package pool

import (
	"context"
	"errors"
	"time"
)

type Publisher struct {
	pool           *Pool
	autoClosePool  bool
	publishTimeout time.Duration
	confirmTimeout time.Duration

	ctx    context.Context
	cancel context.CancelFunc
}

func (p *Publisher) Close() {
	p.cancel()

	if p.autoClosePool {
		p.pool.Close()
	}
}

func NewPublisher(p *Pool, options ...PublisherOption) *Publisher {
	if p == nil {
		panic("nil pool passed")
	}

	// sane defaults, prefer fault tolerance over performance
	option := publisherOption{
		Ctx:            p.Context(),
		PublishTimeout: 15 * time.Second,
		ConfirmTimeout: 15 * time.Second,
		AutoClosePool:  false,
	}

	for _, o := range options {
		o(&option)
	}

	ctx, cancel := context.WithCancel(option.Ctx)

	pub := &Publisher{
		pool:           p,
		autoClosePool:  option.AutoClosePool,
		confirmTimeout: option.ConfirmTimeout,

		ctx:    ctx,
		cancel: cancel,
	}

	return pub
}

func (p *Publisher) Publish(exchange string, routingKey string, mandatory bool, immediate bool, msg Publishing) error {

	for {
		err := p.publish(exchange, routingKey, mandatory, immediate, msg)
		if err == nil {
			return nil
		} else if errors.Is(err, ErrClosed) {
			return err
		}
		// continue in any other error case
	}
}

func (p *Publisher) publish(exchange string, routingKey string, mandatory bool, immediate bool, msg Publishing) (err error) {
	s, err := p.pool.GetSession()
	if err != nil && errors.Is(err, ErrClosed) {
		return ErrClosed
	}
	defer func() {
		// return session
		if err == nil || !errors.Is(err, ErrClosed) {
			p.pool.ReturnSession(s, false)
		} else {
			p.pool.ReturnSession(s, true)
		}
	}()

	pubCtx, pubCancel := context.WithTimeout(p.ctx, p.publishTimeout)
	defer pubCancel()

	tag, err := s.Publish(pubCtx, exchange, routingKey, mandatory, immediate, msg)
	if err != nil && errors.Is(err, ErrClosed) {
		return err
	}

	if !s.IsConfirmable() {
		return nil
	}

	confirmCtx, confirmCancel := context.WithTimeout(p.ctx, p.confirmTimeout)
	defer confirmCancel()

	return s.AwaitConfirm(confirmCtx, tag)
}
