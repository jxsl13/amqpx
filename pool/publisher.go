package pool

import (
	"context"
	"errors"
	"sync"

	"github.com/jxsl13/amqpx/logging"
)

type Publisher struct {
	pool          *Pool
	autoClosePool bool

	ctx    context.Context
	cancel context.CancelFunc

	mu sync.Mutex

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
		Ctx: p.Context(),

		AutoClosePool: false,
		Logger:        p.sp.log, // derive logger from session pool
	}

	for _, o := range options {
		o(&option)
	}

	ctx, cancel := context.WithCancel(option.Ctx)

	pub := &Publisher{
		pool:          p,
		autoClosePool: option.AutoClosePool,
		ctx:           ctx,
		cancel:        cancel,

		log: option.Logger,
	}

	pub.infoSimple("publisher initialized")
	return pub
}

// Publish a message to a specific exchange with a given routingKey.
// You may set exchange to "" and routingKey to your queue name in order to publish directly to a queue.
func (p *Publisher) Publish(ctx context.Context, exchange string, routingKey string, msg Publishing) error {

	for {
		err := p.publish(ctx, exchange, routingKey, msg)
		switch {
		case err == nil:
			return nil
		case errors.Is(err, context.Canceled):
			return err
		case errors.Is(err, context.DeadlineExceeded):
			return err
		case errors.Is(err, ErrClosed):
			return err
		case errors.Is(err, ErrNack):
			return err
		case errors.Is(err, ErrDeliveryTagMismatch):
			return err
		default:
			p.warn(exchange, routingKey, err, "publish failed, retrying")
		}
	}
}

func (p *Publisher) publish(ctx context.Context, exchange string, routingKey string, msg Publishing) (err error) {
	defer func() {
		if err != nil {
			p.warn(exchange, routingKey, err)
		} else {
			p.info(exchange, routingKey, "published a message")
		}
	}()

	s, err := p.pool.GetSession(ctx)
	if err != nil && errors.Is(err, ErrClosed) {
		return err
	}
	defer func() {
		// return session
		if err == nil {
			p.pool.ReturnSession(ctx, s, false)
		} else if errors.Is(err, ErrClosed) {
			// TODO: potential message loss upon shutdown
			// might try a transient session for this one
			p.pool.ReturnSession(ctx, s, false)
		} else {
			p.pool.ReturnSession(ctx, s, true)
		}
	}()

	tag, err := s.Publish(ctx, exchange, routingKey, msg)
	if err != nil {
		return err
	}

	if !s.IsConfirmable() {
		return nil
	}

	return s.AwaitConfirm(ctx, tag)
}

// Get is only supposed to be used for testing, do not use get for polling any broker queues.
func (p *Publisher) Get(ctx context.Context, queue string, autoAck bool) (msg Delivery, ok bool, err error) {
	s, err := p.pool.GetSession(ctx)
	if err != nil && errors.Is(err, ErrClosed) {
		return Delivery{}, false, err
	}
	defer func() {
		// return session
		if err == nil {
			p.pool.ReturnSession(ctx, s, false)
		} else if errors.Is(err, ErrClosed) {
			p.pool.ReturnSession(ctx, s, false)
		} else {
			p.pool.ReturnSession(ctx, s, true)
		}
	}()

	return s.Get(ctx, queue, autoAck)
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
