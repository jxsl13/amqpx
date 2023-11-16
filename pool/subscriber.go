package pool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jxsl13/amqpx/logging"
)

type Subscriber struct {
	pool          *Pool
	autoClosePool bool

	mu            sync.Mutex
	started       bool
	handlers      []*Handler
	batchHandlers []*BatchHandler

	ctx    context.Context
	cancel context.CancelFunc

	wg sync.WaitGroup

	log logging.Logger
}

func (s *Subscriber) Close() {
	s.debugSimple("closing subscriber...")
	defer s.infoSimple("closed")

	s.cancel()
	s.wg.Wait()

	if s.autoClosePool {
		s.pool.Close()
	}
}

// Wait waits until all consumers have been closed.
// The provided context must have been closed in order for Wait to unlock after
// all consumer goroutines of the subscriber have been closed.
func (s *Subscriber) Wait() {
	s.wg.Wait()
}

func NewSubscriber(p *Pool, options ...SubscriberOption) *Subscriber {
	if p == nil {
		panic("nil pool passed")
	}

	// sane defaults, prefer fault tolerance over performance
	option := subscriberOption{
		Ctx:           p.Context(),
		AutoClosePool: false,

		Logger: p.sp.log, // derive logger from session pool
	}

	for _, o := range options {
		o(&option)
	}

	// decouple from parent in order to individually close the context
	ctx, cancel := context.WithCancel(option.Ctx)

	sub := &Subscriber{
		pool:          p,
		autoClosePool: option.AutoClosePool,
		ctx:           ctx,
		cancel:        cancel,

		log: option.Logger,
	}

	return sub
}

// HandlerFunc is basically a handler for incoming messages/events.
type HandlerFunc func(Delivery) error

// BatchHandlerFunc is a handler for incoming batches of messages/events
type BatchHandlerFunc func([]Delivery) error

// RegisterHandlerFunc registers a consumer function that starts a consumer upon subscriber startup.
// The consumer is identified by a string that is unique and scoped for all consumers on this channel.
// An empty string will cause the library to generate a unique identity.
// The consumer identity will be included in every Delivery in the ConsumerTag field
//
// When autoAck (also known as noAck) is true, the server will acknowledge deliveries to this consumer prior to writing the delivery to the network. When autoAck is true, the consumer should not call Delivery.Ack.
// Automatically acknowledging deliveries means that some deliveries may get lost if the consumer is unable to process them after the server delivers them. See http://www.rabbitmq.com/confirms.html for more details.
//
// When exclusive is true, the server will ensure that this is the sole consumer from this queue. When exclusive is false, the server will fairly distribute deliveries across multiple consumers.
//
// The noLocal flag is not supported by RabbitMQ.
// It's advisable to use separate connections for Channel.Publish and Channel.Consume so not to have TCP pushback on publishing affect the ability to consume messages, so this parameter is here mostly for completeness.
//
// When noWait is true, do not wait for the server to confirm the request and immediately begin deliveries. If it is not possible to consume, a channel exception will be raised and the channel will be closed.
// Optional arguments can be provided that have specific semantics for the queue or server.
//
// Inflight messages, limited by Channel.Qos will be buffered until received from the returned chan.
// When the Channel or Connection is closed, all buffered and inflight messages will be dropped.
// When the consumer identifier tag is cancelled, all inflight messages will be delivered until the returned chan is closed.
func (s *Subscriber) RegisterHandlerFunc(queue string, hf HandlerFunc, options ...ConsumeOptions) *Handler {
	option := ConsumeOptions{
		ConsumerTag: "",
		AutoAck:     false,
		Exclusive:   false,
		NoLocal:     false,
		NoWait:      false,
		Args:        nil,
	}
	if len(options) > 0 {
		option = options[0]
	}

	handler := NewHandler(queue, hf, option)
	s.RegisterHandler(handler)
	return handler
}

func (s *Subscriber) RegisterHandler(handler *Handler) {

	s.mu.Lock()
	defer s.mu.Unlock()
	s.handlers = append(s.handlers, handler)

	s.log.WithFields(withConsumerIfSet(handler.ConsumeOptions().ConsumerTag,
		map[string]any{
			"subscriber": s.pool.Name(),
			"queue":      handler.Queue(),
		})).Info("registered message handler")
}

// RegisterBatchHandlerFunc registers a function that is able to process up to `maxBatchSize` messages at the same time.
// The flushTimeout is the duration to wait before triggering the processing of the messages.
// In case your maxBatchSize is 50 and there are only 20 messages in a queue which you can fetch
// and then you'd have to wait indefinitly for those 20 messages to be processed, as it might take a long time for another message to arrive in the queue.
// This is where your flushTimeout comes into play. In order to wait at most for the period of flushTimeout until a new message arrives
// before processing the batch in your handler function.
func (s *Subscriber) RegisterBatchHandlerFunc(queue string, hf BatchHandlerFunc, options ...BatchHandlerOption) *BatchHandler {
	handler := NewBatchHandler(queue, hf, options...)
	s.RegisterBatchHandler(handler)
	return handler
}

// RegisterBatchHandler registers a custom handler that MIGHT not be closed in case that the subscriber is closed.
// The passed batch handler may be derived from a different parent context.
func (s *Subscriber) RegisterBatchHandler(handler *BatchHandler) {

	// TODO: do we want to introduce a BatchSizeLimit
	// which would keep track of accumulated payload memory limits
	// of all the messages of a batch and process the messages
	// in case we hit that memory limit within the current batch.

	s.mu.Lock()
	defer s.mu.Unlock()
	s.batchHandlers = append(s.batchHandlers, handler)

	opts := handler.Config()

	s.log.WithFields(withConsumerIfSet(handler.ConsumeOptions().ConsumerTag,
		map[string]any{
			"subscriber":   s.pool.Name(),
			"queue":        opts.Queue,
			"maxBatchSize": opts.MaxBatchSize, // TODO: optimize so that we don't call getters multiple times (mutex contention)
			"flushTimeout": handler.FlushTimeout,
		})).Info("registered batch message handler")
}

// Start starts the consumers for all registered handler functions
// This method is not blocking. Use Wait() to wait for all routines to shut down
// via context cancelation (e.g. via a signal)
func (s *Subscriber) Start() (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started {
		panic("subscriber cannot be started more than once")
	}

	s.debugSimple("starting subscriber...")
	defer func() {
		if err != nil {
			s.Close()
		} else {
			// after starting everything we want to set started to true
			s.started = true
			s.infoSimple("started")
		}
	}()

	s.debugSimple(fmt.Sprintf("starting %d handler routine(s)", len(s.handlers)))
	for _, h := range s.handlers {
		s.wg.Add(1)
		go s.consumer(h, &s.wg)
		err = h.awaitResumed(s.ctx)
		if err != nil {
			return fmt.Errorf("failed to start consumer for queue %s: %w", h.Queue(), err)
		}
	}

	s.debugSimple(fmt.Sprintf("starting %d batch handler routine(s)", len(s.batchHandlers)))
	for _, bh := range s.batchHandlers {
		s.wg.Add(1)
		go s.batchConsumer(bh, &s.wg)

		err = bh.awaitResumed(s.ctx)
		if err != nil {
			return fmt.Errorf("failed to start batch consumer for queue %s: %w", bh.Queue(), err)
		}
	}
	return nil
}

func (s *Subscriber) consumer(h *Handler, wg *sync.WaitGroup) {
	defer wg.Done()
	defer h.close()

	var err error
	// trigger initial startup
	// channel below
	opts, err := h.start(s.ctx)
	if err != nil {
		s.error(opts.ConsumerTag, opts.Queue, err, "failed to start consumer")
		return
	}

	for {
		select {
		case <-s.catchShutdown():
			return
		case <-h.resuming().Done():
			err = s.consume(h)
			if errors.Is(err, ErrClosed) {
				return
			}
		}
	}
}

func (s *Subscriber) consume(h *Handler) (err error) {
	opts, err := h.start(s.ctx)
	if err != nil {
		return err
	}
	defer h.paused()

	s.debugConsumer(opts.ConsumerTag, "starting consumer...")

	session, err := s.pool.GetSession()
	if err != nil {
		return err
	}
	defer func() {
		// err evaluation upon defer
		s.returnSession(h, session, err)
	}()

	// got a working session
	delivery, err := session.ConsumeWithContext(
		h.pausing().Context(),
		opts.Queue,
		opts.ConsumeOptions,
	)
	if err != nil {
		return err
	}

	h.resumed()
	s.infoConsumer(opts.ConsumerTag, "started")
	for {
		select {
		case <-s.catchShutdown():
			return ErrClosed
		case msg, ok := <-delivery:
			if !ok {
				return ErrDeliveryClosed
			}

			s.infoHandler(opts.ConsumerTag, msg.Exchange, msg.RoutingKey, opts.Queue, "received message")
			err = opts.HandlerFunc(msg)
			if opts.AutoAck {
				if err != nil {
					// we cannot really do anything to recover from a processing error in this case
					s.errorHandler(opts.ConsumerTag, msg.Exchange, msg.RoutingKey, opts.Queue, fmt.Errorf("processing failed: dropping message: %w", err))
				} else {
					s.infoHandler(opts.ConsumerTag, msg.Exchange, msg.RoutingKey, opts.Queue, "processed message")
				}
			} else {
				poolErr := s.ackPostHandle(opts, msg.DeliveryTag, msg.Exchange, msg.RoutingKey, session, err)
				if poolErr != nil {
					return poolErr
				}
			}
		}
	}
}

// (n)ack delivery and signal that message was processed by the service
func (s *Subscriber) ackPostHandle(opts HandlerConfig, deliveryTag uint64, exchange, routingKey string, session *Session, handlerErr error) (err error) {
	var ackErr error
	if handlerErr != nil {
		// requeue message if possible
		ackErr = session.Nack(deliveryTag, false, true)
	} else {
		ackErr = session.Ack(deliveryTag, false)
	}

	// if (n)ack fails, we know that the connection died
	// potentially before processing already.
	if ackErr != nil {
		s.warnHandler(opts.ConsumerTag, exchange, routingKey, opts.Queue, ackErr, "(n)ack failed")
		poolErr := session.Recover()
		if poolErr != nil {
			// only returns an error upon shutdown
			return poolErr
		}

		// do not retry, because the broker will requeue the un(n)acked message
		// after a timeout of (by default) 30 minutes.
		return nil
	}

	// (n)acked successfully
	if handlerErr != nil {
		s.infoHandler(opts.ConsumerTag, exchange, routingKey, opts.Queue, "nacked message")
	} else {
		s.infoHandler(opts.ConsumerTag, exchange, routingKey, opts.Queue, "acked message")
	}
	// successfully handled message
	return nil
}

func (s *Subscriber) batchConsumer(h *BatchHandler, wg *sync.WaitGroup) {
	defer wg.Done()
	defer h.close()

	var err error
	// initialize all handler contexts
	// to be in state resuming
	opts, err := h.start(s.ctx)
	if err != nil {
		s.error(opts.ConsumerTag, opts.Queue, err, "failed to start batch handler consumer")
		return
	}

	for {
		select {
		case <-s.catchShutdown():
			return
		case <-h.resuming().Done():
			err = s.batchConsume(h)
			if errors.Is(err, ErrClosed) {
				return
			}
		}
	}
}

func (s *Subscriber) batchConsume(h *BatchHandler) (err error) {
	opts, err := h.start(s.ctx)
	if err != nil {
		return err
	}
	defer h.paused()

	s.debugConsumer(opts.ConsumerTag, "starting batch consumer...")

	session, err := s.pool.GetSession()
	if err != nil {
		return err
	}
	defer func() {
		// err evaluation upon defer
		s.returnSession(h, session, err)
	}()

	// got a working session
	delivery, err := session.ConsumeWithContext(
		h.pausing().Context(),
		opts.Queue,
		opts.ConsumeOptions,
	)
	if err != nil {
		return err
	}

	h.resumed()
	s.infoConsumer(opts.ConsumerTag, "started")

	// preallocate memory for batch
	batch := make([]Delivery, 0, maxi(1, opts.MaxBatchSize))
	defer func() {
		if len(batch) > 0 && errors.Is(err, ErrClosed) {
			// requeue all not yet processed messages in batch slice
			// we can only nack these message sin case the session has not been closed
			// Its does not make sense to use session.Nack at this point because in case the sessison dies
			// the delivery tags also die with it.
			// There is no way to recover form this state in case an error is returned from the Nack call.
			nackErr := batch[len(batch)-1].Nack(true, true)
			if nackErr != nil {
				s.warnBatchHandler(opts.ConsumerTag, opts.Queue, opts.MaxBatchSize, err, "failed to nack and requeue batch upon shutdown")
			}
		}
	}()

	var (
		timer   = time.NewTimer(opts.FlushTimeout)
		drained = false
	)
	defer closeTimer(timer, &drained)

	for {
		// reset batch slice
		// reuse memory
		batch = batch[:0]

	collectBatch:
		for {

			// reset the timer
			resetTimer(timer, opts.FlushTimeout, &drained)

			select {
			case <-s.catchShutdown():
				return ErrClosed
			case msg, ok := <-delivery:
				if !ok {
					return ErrDeliveryClosed
				}
				batch = append(batch, msg)
				if len(batch) == opts.MaxBatchSize {
					break collectBatch
				}

			case <-timer.C:
				// at this point we know that the timer channel has been drained
				drained = true
				if len(batch) > 0 {
					// timeout reached, process batch that might not contain
					// a full batch, yet.
					break collectBatch
				}

				// no new messages, continue, reset timer and try again
			}
		}

		// at this point we have a batch to work with
		var (
			batchSize       = len(batch)
			lastDeliveryTag = batch[len(batch)-1].DeliveryTag
		)

		s.infoBatchHandler(opts.ConsumerTag, opts.Queue, batchSize, "received batch")
		err = opts.HandlerFunc(batch)
		// no acks required
		if opts.AutoAck {
			if err != nil {
				// we cannot really do anything to recover from a processing error in this case
				s.errorBatchHandler(opts.ConsumerTag, opts.Queue, batchSize, fmt.Errorf("processing failed: dropping batch: %w", err))
			} else {
				s.infoBatchHandler(opts.ConsumerTag, opts.Queue, batchSize, "processed batch")
			}
		} else {
			poolErr := s.ackBatchPostHandle(opts, lastDeliveryTag, batchSize, session, err)
			if poolErr != nil {
				return poolErr
			}
		}
	}
}

func (s *Subscriber) ackBatchPostHandle(opts BatchHandlerConfig, lastDeliveryTag uint64, currentBatchSize int, session *Session, handlerErr error) (err error) {
	var ackErr error
	// processing failed
	if handlerErr != nil {
		// requeue message if possible & nack all previous messages
		ackErr = session.Nack(lastDeliveryTag, true, true)
	} else {
		// ack last and all previous messages
		ackErr = session.Ack(lastDeliveryTag, true)
	}

	// if (n)ack fails, we know that the connection died
	// potentially before processing already.
	if ackErr != nil {
		s.warnBatchHandler(opts.ConsumerTag, opts.Queue, currentBatchSize, ackErr, "batch (n)ack failed")
		poolErr := session.Recover()
		if poolErr != nil {
			// only returns an error upon shutdown
			return poolErr
		}

		// do not retry, because the broker will requeue the un(n)acked message
		// after a timeout of by default 30 minutes.
		return nil
	}

	// (n)acked successfully
	if handlerErr != nil {
		s.infoBatchHandler(opts.ConsumerTag, opts.Queue, currentBatchSize, "nacked batch")
	} else {
		s.infoBatchHandler(opts.ConsumerTag, opts.Queue, currentBatchSize, "acked batch")
	}
	// successfully handled message
	return nil
}

type handler interface {
	ConsumeOptions() ConsumeOptions
	Queue() string
	pausing() doner
}

func (s *Subscriber) returnSession(h handler, session *Session, err error) {
	opts := h.ConsumeOptions()

	if errors.Is(err, ErrClosed) {
		// graceful shutdown
		s.pool.ReturnSession(session, false)
		s.infoConsumer(opts.ConsumerTag, "closed")
		return
	}

	select {
	case <-h.pausing().Done():
		// expected closing due to context cancelation
		// cancel errors the underlying channel
		// A canceled session is an erred session.
		s.pool.ReturnSession(session, true)
		s.infoConsumer(opts.ConsumerTag, "paused")
	default:
		// actual error
		s.pool.ReturnSession(session, true)
		s.warnConsumer(opts.ConsumerTag, err, "closed unexpectedly")
	}
}

func (s *Subscriber) catchShutdown() <-chan struct{} {
	return s.ctx.Done()
}

func (s *Subscriber) infoBatchHandler(consumer, queue string, batchSize int, a ...any) {
	s.log.WithFields(withConsumerIfSet(consumer,
		map[string]any{
			"batchSize":  batchSize,
			"subscriber": s.pool.Name(),
			"queue":      queue,
		})).Info(a...)
}

func (s *Subscriber) warnBatchHandler(consumer, queue string, batchSize int, err error, a ...any) {
	s.log.WithFields(withConsumerIfSet(consumer, map[string]any{
		"batchSize":  batchSize,
		"subscriber": s.pool.Name(),
		"queue":      queue,
		"error":      err,
	})).Warn(a...)
}

func (s *Subscriber) errorBatchHandler(consumer, queue string, batchSize int, err error, a ...any) {
	s.log.WithFields(withConsumerIfSet(consumer,
		map[string]any{
			"batchSize":  batchSize,
			"subscriber": s.pool.Name(),
			"queue":      queue,
			"error":      err,
		})).Error(a...)
}

func (s *Subscriber) infoHandler(consumer, exchange, routingKey, queue string, a ...any) {
	s.log.WithFields(withConsumerIfSet(consumer, map[string]any{
		"subscriber": s.pool.Name(),
		"exchange":   exchange,
		"routingKey": routingKey,
		"queue":      queue,
	})).Info(a...)
}

func (s *Subscriber) warnHandler(consumer, exchange, routingKey, queue string, err error, a ...any) {
	s.log.WithFields(withConsumerIfSet(consumer, map[string]any{
		"subscriber": s.pool.Name(),
		"exchange":   exchange,
		"routingKey": routingKey,
		"queue":      queue,
		"error":      err,
	})).Warn(a...)
}

func (s *Subscriber) errorHandler(consumer, exchange, routingKey, queue string, err error, a ...any) {
	s.log.WithFields(withConsumerIfSet(consumer, map[string]any{
		"subscriber": s.pool.Name(),
		"exchange":   exchange,
		"routingKey": routingKey,
		"queue":      queue,
		"error":      err,
	})).Error(a...)
}

func (s *Subscriber) infoSimple(a ...any) {
	s.log.WithFields(map[string]any{
		"subscriber": s.pool.Name(),
	}).Info(a...)
}

func (s *Subscriber) debugSimple(a ...any) {
	s.log.WithFields(map[string]any{
		"subscriber": s.pool.Name(),
	}).Debug(a...)
}

func (s *Subscriber) debugConsumer(consumer string, a ...any) {
	s.log.WithFields(withConsumerIfSet(consumer, map[string]any{
		"subscriber": s.pool.Name(),
	})).Debug(a...)
}

func (s *Subscriber) warnConsumer(consumer string, err error, a ...any) {
	s.log.WithFields(withConsumerIfSet(consumer, map[string]any{
		"subscriber": s.pool.Name(),
		"error":      err,
	})).Warn(a...)
}

func (s *Subscriber) infoConsumer(consumer string, a ...any) {
	s.log.WithFields(withConsumerIfSet(consumer, map[string]any{
		"subscriber": s.pool.Name(),
	})).Info(a...)
}

func withConsumerIfSet(consumer string, m map[string]any) map[string]any {
	if consumer != "" {
		m["consumer"] = consumer
	}
	return m
}

func (s *Subscriber) error(consumer, queue string, err error, a ...any) {
	s.log.WithFields(withConsumerIfSet(consumer, map[string]any{
		"subscriber": s.pool.Name(),
		"queue":      queue,
		"error":      err,
	})).Error(a...)
}
