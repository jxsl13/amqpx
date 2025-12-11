package pool

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jxsl13/amqpx/internal/contextutils"
	"github.com/jxsl13/amqpx/internal/errorutils"
	"github.com/jxsl13/amqpx/internal/timerutils"
	"github.com/jxsl13/amqpx/types"
)

type Publisher struct {
	pool          *Pool
	autoClosePool bool
	backoff       types.BackoffFunc

	ctx    context.Context
	cancel context.CancelFunc

	log *slog.Logger
}

func (p *Publisher) Close() {
	log := p.splog()
	log.Debug("closing publisher...")
	defer log.Info("publisher closed")

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

	pub.splog().Info("publisher initialized")
	return pub
}

// Publish a message to a specific exchange with a given routingKey.
// You may set exchange to "" and routingKey to your queue name in order to publish directly to a queue.
func (p *Publisher) Publish(ctx context.Context, exchange string, routingKey string, msg types.Publishing) error {
	// does not change throughout the function
	log := p.eplog(exchange, routingKey)

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
			log.Warn(fmt.Sprintf("publish failed due to recoverable error, retrying: %s", err.Error()))
			return true, nil
		}
	})
}

func (p *Publisher) publish(ctx context.Context, exchange string, routingKey string, msg types.Publishing) (err error) {
	defer func() {
		log := p.eplog(exchange, routingKey)
		if err != nil {
			log.Error(fmt.Sprintf("failed to publish message: %s", err.Error()))
		} else {
			log.Info("published a message")
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

// PublishBatch a message to a specific exchange with a given routingKey.
// You may set exchange to "" and routingKey to your queue name in order to publish directly to a queue.
func (p *Publisher) PublishBatch(ctx context.Context, exchange string, routingKey string, msgs []types.Publishing) error {
	// does not change throughout the function
	log := p.eplog(exchange, routingKey)

	return p.retry(ctx, func() (cont bool, err error) {
		err = p.publishBatch(ctx, exchange, routingKey, msgs)
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
			log.Warn(fmt.Sprintf("publish failed due to recoverable error, retrying: %s", err.Error()))
			return true, nil
		}
	})
}

func (p *Publisher) publishBatch(ctx context.Context, exchange string, routingKey string, msgs []types.Publishing) (err error) {
	defer func() {
		log := p.eplog(exchange, routingKey)
		if err != nil {
			log.Error(fmt.Sprintf("failed to publish batch: %s", err.Error()))
		} else {
			log.Info("published a batch with %d messages", slog.Int("batch_size", len(msgs)))
		}
	}()

	s, err := p.pool.ForceGetSession(ctx)
	if err != nil {
		return err
	}
	defer func() {
		p.pool.ReturnSession(s, err)
	}()

	deliveryTags, err := s.PublishBatch(ctx, exchange, routingKey, msgs)
	if err != nil {
		return err
	}

	if !s.IsConfirmable() {
		return nil
	}
	for _, tag := range deliveryTags {
		err = s.AwaitConfirm(ctx, tag)
		if err != nil {
			return err
		}
	}
	return nil
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

func (p *Publisher) eplog(exchange, routingKey string) *slog.Logger {
	return p.log.With(
		slog.String("publisher", p.pool.Name()),
		slog.String("exchange", exchange),
		slog.String("routing_key", routingKey),
	)
}

func (p *Publisher) splog() *slog.Logger {
	return p.log.With(
		slog.String("publisher", p.pool.Name()),
	)
}
