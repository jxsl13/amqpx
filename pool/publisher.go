package pool

import (
	"context"
	"errors"
	"fmt"

	"github.com/jxsl13/amqpx/logging"
)

type Publisher struct {
	pool          *Pool
	autoClosePool bool

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
		Ctx: p.Context(),

		AutoClosePool: false,
		Logger:        p.sp.log, // derive logger from session pool
	}

	for _, o := range options {
		o(&option)
	}

	ctx, cc := context.WithCancelCause(option.Ctx)
	cancel := toCancelFunc(fmt.Errorf("publisher %w", ErrClosed), cc)

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

// Publishes a batch of messages.
// Each messages can be published to a different exchange and routing key.
// Due to the asynchronicity, order of messages is not guaranteed when sending multiple messages to the same queue.
func (p *Publisher) PublishBatch(ctx context.Context, msgs []BatchPublishing) error {
	for {
		err := p.publishBatch(ctx, msgs)
		switch {
		case err == nil:
			return nil
		case errors.Is(err, ErrNack):
			return err
		case errors.Is(err, ErrDeliveryTagMismatch):
			return err
		default:
			if recoverable(err) {
				for _, msg := range msgs {
					p.warn(msg.Exchange, msg.RoutingKey, err, "publish failed due to recoverable error, retrying")
				}
				// retry
			} else {
				return err
			}
		}
	}
}

// Publish a message to a specific exchange with a given routingKey.
// You may set exchange to "" and routingKey to your queue name in order to publish directly to a queue.
func (p *Publisher) Publish(ctx context.Context, exchange string, routingKey string, msg Publishing) error {

	for {
		err := p.publish(ctx, exchange, routingKey, msg)
		switch {
		case err == nil:
			return nil
		case errors.Is(err, ErrNack):
			return err
		case errors.Is(err, ErrDeliveryTagMismatch):
			return err
		default:
			if recoverable(err) {
				p.warn(exchange, routingKey, err, "publish failed due to recoverable error, retrying")
				// retry
			} else {
				return err
			}
		}
	}
}

func (p *Publisher) publishBatch(ctx context.Context, msgs []BatchPublishing) (err error) {
	defer func() {
		if err != nil {
			for _, msg := range msgs {
				p.warn(msg.Exchange, msg.RoutingKey, err, "failed to publish message")
			}
		} else {
			for _, msg := range msgs {
				p.info(msg.Exchange, msg.RoutingKey, "published a message")
			}
		}
	}()

	s, err := p.pool.GetSession(ctx)
	if err != nil {
		return err
	}
	defer func() {
		p.pool.ReturnSession(s, err)
	}()

	confirm, err := s.PublishBatch(ctx, msgs)
	if err != nil {
		return err
	}

	if !s.IsConfirmable() {
		return nil
	}

	return confirm.Wait(ctx)
}

func (p *Publisher) publish(ctx context.Context, exchange string, routingKey string, msg Publishing) (err error) {
	defer func() {
		if err != nil {
			p.warn(exchange, routingKey, err, "failed to publish message")
		} else {
			p.info(exchange, routingKey, "published a message")
		}
	}()

	s, err := p.pool.GetSession(ctx)
	if err != nil {
		return err
	}
	defer func() {
		p.pool.ReturnSession(s, err)
	}()

	confirm, err := s.Publish(ctx, exchange, routingKey, msg)
	if err != nil {
		return err
	}

	if !s.IsConfirmable() {
		return nil
	}

	return confirm.Wait(ctx)
}

// Get is only supposed to be used for testing, do not use get for polling any broker queues.
func (p *Publisher) Get(ctx context.Context, queue string, autoAck bool) (msg Delivery, ok bool, err error) {
	s, err := p.pool.GetSession(ctx)
	if err != nil {
		return Delivery{}, false, err
	}
	defer func() {
		p.pool.ReturnSession(s, err)
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
