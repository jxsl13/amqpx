package pool

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jxsl13/amqpx/internal/contextutils"
	"github.com/jxsl13/amqpx/internal/errorutils"
	"github.com/jxsl13/amqpx/internal/timerutils"
	"github.com/jxsl13/amqpx/logging"
	"github.com/jxsl13/amqpx/types"
)

type Publisher struct {
	pool          *Pool
	autoClosePool bool
	backoff       types.BackoffFunc

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
		BackoffPolicy: types.NewBackoffPolicy(1*time.Millisecond, 5*time.Second), // currently only affects publishing with confirms
		Logger:        p.sp.log,                                                  // derive logger from session pool
	}

	for _, o := range options {
		o(&option)
	}

	ctx, cc := context.WithCancelCause(option.Ctx)
	cancel := contextutils.ToCancelFunc(fmt.Errorf("publisher %w", types.ErrClosed), cc)

	pub := &Publisher{
		pool:          p,
		autoClosePool: option.AutoClosePool,
		backoff:       option.BackoffPolicy,
		ctx:           ctx,
		cancel:        cancel,

		log: option.Logger,
	}

	pub.infoSimple("publisher initialized")
	return pub
}

// Publish a message to a specific exchange with a given routingKey.
// You may set exchange to "" and routingKey to your queue name in order to publish directly to a queue.
func (p *Publisher) Publish(ctx context.Context, exchange string, routingKey string, msg types.Publishing) error {
	return p.retry(ctx, func() (cont bool, err error) {
		err = p.publish(ctx, exchange, routingKey, msg)
		switch {
		case err == nil:
			return false, nil
		case errors.Is(err, types.ErrNack):
			return false, err
		case !errorutils.Recoverable(err):
			// not recoverable
			return false, err
		default:
			// ErrDeliveryTagMismatch + all other unknown errors
			p.warn(exchange, routingKey, err, "publish failed due to recoverable error, retrying")
			return true, nil
		}
	})
}

func (p *Publisher) publish(ctx context.Context, exchange string, routingKey string, msg types.Publishing) (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("publish failed: %w", err)
			p.warn(exchange, routingKey, err, "failed to publish message")
		} else {
			p.info(exchange, routingKey, "published a message")
		}
	}()

	s, err := p.pool.ForceGetSession(ctx)
	if err != nil {
		return err
	}
	defer func() {
		p.pool.ReturnSession(s, err)
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

func (p *Publisher) retry(ctx context.Context, f func() (cont bool, err error)) error {
	// fast path
	cont, err := f()
	if err != nil {
		return err
	}
	if !cont {
		return nil
	}

	// continue second try with backoff timer overhead
	var (
		retry   = 1
		timer   = time.NewTimer(p.backoff(retry))
		drained = false
	)
	defer timerutils.CloseTimer(timer, &drained)

	for {

		select {
		case <-timer.C:
			// at this point we know that the timer channel has been drained
			drained = true

			// try again
			cont, err = f()
			if err != nil {
				return err
			}
			if !cont {
				return nil
			}

			retry++
			timerutils.ResetTimer(timer, p.backoff(retry), &drained)

		case <-ctx.Done():
			return errors.Join(err, ctx.Err())
		case <-p.ctx.Done():
			return errors.Join(err, p.ctx.Err())
		}
	}
}

// Get is only supposed to be used for testing, do not use get for polling any broker queues.
func (p *Publisher) Get(ctx context.Context, queue string, autoAck bool) (msg types.Delivery, ok bool, err error) {
	s, err := p.pool.GetSession(ctx)
	if err != nil {
		return types.Delivery{}, false, err
	}
	defer func() {
		p.pool.ReturnSession(s, err)
	}()

	return s.Get(ctx, queue, autoAck)
}

func (p *Publisher) info(exchange, routingKey string, a ...any) {
	p.log.WithFields(map[string]any{
		"publisher":   p.pool.Name(),
		"exchange":    exchange,
		"routing_key": routingKey,
	}).Info(a...)
}

func (p *Publisher) warn(exchange, routingKey string, err error, a ...any) {
	p.log.WithFields(map[string]any{
		"publisher":   p.pool.Name(),
		"exchange":    exchange,
		"routing_key": routingKey,
		"error":       err,
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
