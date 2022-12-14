package pool

import (
	"context"
	"errors"
	"time"

	"github.com/jxsl13/amqpx/logging"
	"github.com/rabbitmq/amqp091-go"
)

type Publisher struct {
	pool           *Pool
	autoClosePool  bool
	publishTimeout time.Duration
	confirmTimeout time.Duration

	ctx    context.Context
	cancel context.CancelFunc

	log logging.Logger
}

func (p *Publisher) Close() {
	p.debugSimple("closing publisher...")
	defer p.infoSimple("closed")

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
		Logger:         p.sp.log, // derive logger from session pool
	}

	for _, o := range options {
		o(&option)
	}

	ctx, cancel := context.WithCancel(option.Ctx)

	pub := &Publisher{
		pool:           p,
		autoClosePool:  option.AutoClosePool,
		confirmTimeout: option.ConfirmTimeout,
		publishTimeout: option.PublishTimeout,

		ctx:    ctx,
		cancel: cancel,

		log: option.Logger,
	}

	pub.infoSimple("publisher initialized")
	return pub
}

func (p *Publisher) Publish(exchange string, routingKey string, msg Publishing) error {

	for {
		err := p.publish(exchange, routingKey, msg)
		if err == nil {
			return nil
		} else if errors.Is(err, ErrClosed) {
			return err
		} else {
			p.warn(exchange, routingKey, err, "publish failed, retrying")
		}
		// continue in any other error case
	}
}

func (p *Publisher) publish(exchange string, routingKey string, msg Publishing) (err error) {
	defer func() {
		if err != nil {
			p.warn(exchange, routingKey, err)
		} else {
			p.info(exchange, routingKey, "published a message")
		}
	}()

	s, err := p.pool.GetSession()
	if err != nil && errors.Is(err, ErrClosed) {
		return ErrClosed
	}
	defer func() {
		// return session
		if err == nil {
			p.pool.ReturnSession(s, false)
		} else if errors.Is(err, ErrClosed) {
			// TODO: potential message loss upon shutdown
			// might try a transient session for this one
			p.pool.ReturnSession(s, false)
		} else {
			p.pool.ReturnSession(s, true)
		}
	}()

	pubCtx, pubCancel := context.WithTimeout(p.ctx, p.publishTimeout)
	defer pubCancel()

	tag, err := s.Publish(pubCtx, exchange, routingKey, msg)
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

// Get is only supposed to be used for testing, do not use get for polling any broker queues.
func (p *Publisher) Get(queue string, autoAck bool) (msg *amqp091.Delivery, ok bool, err error) {
	s, err := p.pool.GetSession()
	if err != nil && errors.Is(err, ErrClosed) {
		return nil, false, ErrClosed
	}
	defer func() {
		// return session
		if err == nil {
			p.pool.ReturnSession(s, false)
		} else if errors.Is(err, ErrClosed) {
			p.pool.ReturnSession(s, false)
		} else {
			p.pool.ReturnSession(s, true)
		}
	}()

	return s.Get(queue, autoAck)
}

func (p *Publisher) info(exchange, routingKey string, a ...any) {
	p.log.WithFields(map[string]any{
		"publisher":  p.pool.Name(),
		"exchange":   exchange,
		"routingKey": routingKey,
	}).Info(a...)
}

func (p *Publisher) warn(exchange, routingKey string, err error, a ...any) {
	p.log.WithFields(map[string]any{
		"publisher":  p.pool.Name(),
		"exchange":   exchange,
		"routingKey": routingKey,
		"error":      err,
	}).Warn(a...)
}

func (p *Publisher) infoSimple(a ...any) {
	p.log.WithFields(map[string]any{
		"publisher": p.pool.Name(),
	}).Info(a...)
}

func (p *Publisher) debugSimple(a ...any) {
	p.log.WithFields(map[string]any{
		"publisher": p.pool.Name(),
	}).Debug(a...)
}
