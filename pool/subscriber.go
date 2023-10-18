package pool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jxsl13/amqpx/logging"
	"github.com/rabbitmq/amqp091-go"
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

	ctx, cancel := context.WithCancel(option.Ctx)

	sub := &Subscriber{
		pool:          p,
		autoClosePool: option.AutoClosePool,

		ctx:    ctx,
		cancel: cancel,

		log: option.Logger,
	}

	return sub
}

// HandlerFunc is basically a handler for incoming messages/events.
type HandlerFunc func(amqp091.Delivery) error

// BatchHandlerFunc is a handler for incoming batches of messages/events
type BatchHandlerFunc func([]amqp091.Delivery) error

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
func (s *Subscriber) RegisterHandlerFunc(queue string, hf HandlerFunc, options ...ConsumeOptions) {
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

	s.RegisterHandler(NewHandler(queue, hf, option))
}

func (s *Subscriber) RegisterHandler(handler *Handler) {

	s.mu.Lock()
	defer s.mu.Unlock()
	s.handlers = append(s.handlers, handler)

	s.log.WithFields(withConsumerIfSet(handler.ConsumeOptions().ConsumerTag,
		map[string]any{
			"subscriber": s.pool.Name(),
			"queue":      handler.Queue,
		})).Info("registered message handler")
}

// RegisterBatchHandlerFunc registers a function that is able to process up to `maxBatchSize` messages at the same time.
// The flushTimeout is the duration to wait before triggering the processing of the messages.
// In case your maxBatchSize is 50 and there are only 20 messages in a queue which you can fetch
// and then you'd have to wait indefinitly for those 20 messages to be processed, as it might take a long time for another message to arrive in the queue.
// This is where your flushTimeout comes into play. In order to wait at most for the period of flushTimeout until a new message arrives
// before processing the batch in your handler function.
func (s *Subscriber) RegisterBatchHandlerFunc(queue string, hf BatchHandlerFunc, options ...BatchHandlerOption) {
	s.RegisterBatchHandler(NewBatchHandler(queue, hf, options...))
}

func (s *Subscriber) RegisterBatchHandler(handler *BatchHandler) {

	// TODO: do we want to introduce a BatchSizeLimit
	// which would keep track of accumulated payload memory limits
	// of all the messages of a batch and process the messages
	// in case we hit that memory limit within the current batch.

	s.mu.Lock()
	defer s.mu.Unlock()
	s.batchHandlers = append(s.batchHandlers, handler)

	s.log.WithFields(withConsumerIfSet(handler.ConsumeOptions().ConsumerTag,
		map[string]any{
			"subscriber":   s.pool.Name(),
			"queue":        handler.Queue(),
			"maxBatchSize": handler.MaxBatchSize(), // TODO: optimize so that we don't call getters multiple times (mutex contention)
			"flushTimeout": handler.FlushTimeout(),
		})).Info("registered batch handler")
}

// Start starts the consumers for all registered handler functions
// This method is not blocking. Use Wait() to wait for all routines to shut down
// via context cancelation (e.g. via a signal)
func (s *Subscriber) Start() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.started {
		panic("subscriber cannot be started more than once")
	}

	s.debugSimple("starting subscriber...")
	defer func() {
		// after starting everything we want to set started to true
		s.started = true
		s.infoSimple("started")
	}()

	s.debugSimple(fmt.Sprintf("starting %d handler routine(s)", len(s.handlers)))
	for _, h := range s.handlers {
		s.wg.Add(1)
		go s.consumer(h, &s.wg)
	}

	s.debugSimple(fmt.Sprintf("starting %d batch handler routine(s)", len(s.batchHandlers)))
	for _, bh := range s.batchHandlers {
		s.wg.Add(1)
		go s.batchConsumer(bh, &s.wg)
	}
}

func (s *Subscriber) consumer(h *Handler, wg *sync.WaitGroup) {
	defer wg.Done()

	var err error
	for {
		err = s.consume(h)
		if errors.Is(err, ErrClosed) {
			return
		}
		select {
		case <-s.catchShutdown():
			return
		case <-h.isRunning():
			// pause logic
			continue
		}
	}
}

func (s *Subscriber) batchConsumer(bh *BatchHandler, wg *sync.WaitGroup) {
	defer wg.Done()

	var err error
	for {
		err = s.batchConsume(bh)
		if errors.Is(err, ErrClosed) {
			return
		}
		select {
		case <-s.catchShutdown():
			return
		case <-bh.isRunning():
			// pause logic
			continue
		}
	}
}

func (s *Subscriber) consume(h *Handler) (err error) {
	view := h.View()

	s.debugConsumer(view.ConsumerTag, "starting consumer...")

	session, err := s.pool.GetSession()
	if err != nil {
		return err
	}
	defer func() {
		if err == nil {
			// no error
			s.pool.ReturnSession(session, false)
			s.infoConsumer(view.ConsumerTag, "closed")
		} else if errors.Is(err, ErrClosed) {
			// graceful shutdown
			s.pool.ReturnSession(session, false)
			s.infoConsumer(view.ConsumerTag, "closed")
		} else {
			// actual error
			s.pool.ReturnSession(session, true)
			s.warnConsumer(view.ConsumerTag, err, "closed unexpectedly")
		}
	}()

	// got a working session
	delivery, err := session.Consume(
		view.Queue,
		view.ConsumeOptions,
	)
	if err != nil {
		return err
	}

	h.started(session)
	s.infoConsumer(view.ConsumerTag, "started")
	for {
		select {
		case <-s.catchShutdown():
			return ErrClosed
		case msg, ok := <-delivery:
			if !ok {
				return ErrDeliveryClosed
			}

			s.infoHandler(view.ConsumerTag, msg.Exchange, msg.RoutingKey, view.Queue, "received message")
			err = view.HandlerFunc(msg)
			if view.AutoAck {
				if err != nil {
					// we cannot really do anything to recover from a processing error in this case
					s.errorHandler(view.ConsumerTag, msg.Exchange, msg.RoutingKey, view.Queue, fmt.Errorf("processing failed: dropping message: %w", err))
				} else {
					s.infoHandler(view.ConsumerTag, msg.Exchange, msg.RoutingKey, view.Queue, "processed message")
				}
			} else {
				poolErr := s.ackPostHandle(&view, msg.DeliveryTag, msg.Exchange, msg.RoutingKey, session, err)
				if poolErr != nil {
					return poolErr
				}
			}
		}
	}
}

// (n)ack delivery and signal that message was processed by the service
func (s *Subscriber) ackPostHandle(view *HandlerView, deliveryTag uint64, exchange, routingKey string, session *Session, handlerErr error) (err error) {
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
		s.warnHandler(view.ConsumerTag, exchange, routingKey, view.Queue, ackErr, "(n)ack failed")
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
		s.infoHandler(view.ConsumerTag, exchange, routingKey, view.Queue, "nacked message")
	} else {
		s.infoHandler(view.ConsumerTag, exchange, routingKey, view.Queue, "acked message")
	}
	// successfully handled message
	return nil
}

func (s *Subscriber) batchConsume(bh *BatchHandler) (err error) {
	view := bh.View()
	s.debugConsumer(view.ConsumerTag, "starting batch consumer...")

	session, err := s.pool.GetSession()
	if err != nil {
		return err
	}
	defer func() {
		if err == nil {
			// no error
			s.pool.ReturnSession(session, false)
			s.infoConsumer(view.ConsumerTag, "closed")
		} else if errors.Is(err, ErrClosed) {
			// graceful shutdown
			s.pool.ReturnSession(session, false)
			s.infoConsumer(view.ConsumerTag, "closed")
		} else {
			// actual error
			s.pool.ReturnSession(session, true)
			s.warnConsumer(view.ConsumerTag, err, "closed unexpectedly")
		}
	}()

	// got a working session
	delivery, err := session.Consume(
		view.Queue,
		view.ConsumeOptions,
	)
	if err != nil {
		return err
	}

	bh.started(session)
	s.infoConsumer(view.ConsumerTag, "started")

	// preallocate memory for batch
	batch := make([]amqp091.Delivery, 0, view.MaxBatchSize)
	defer func() {
		if len(batch) > 0 && errors.Is(err, ErrClosed) {
			// requeue all not yet processed messages in batch slice
			// we can only nack these message sin case the session has not been closed
			// Its does not make sense to use session.Nack at this point because in case the sessison dies
			// the delivery tags also die with it.
			// There is no way to recover form this state in case an error is returned from the Nack call.
			nackErr := batch[len(batch)-1].Nack(true, true)
			if nackErr != nil {
				s.warnBatchHandler(view.ConsumerTag, view.Queue, view.MaxBatchSize, err, "failed to nack and requeue batch upon shutdown")
			}
		}
	}()

	var (
		timer   = time.NewTimer(view.FlushTimeout)
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
			resetTimer(timer, view.FlushTimeout, &drained)

			select {
			case <-s.catchShutdown():
				return ErrClosed
			case msg, ok := <-delivery:
				if !ok {
					return ErrDeliveryClosed
				}
				batch = append(batch, msg)
				if len(batch) == view.MaxBatchSize {
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

		s.infoBatchHandler(view.ConsumerTag, view.Queue, batchSize, "received batch")
		err = view.HandlerFunc(batch)
		// no acks required
		if view.AutoAck {
			if err != nil {
				// we cannot really do anything to recover from a processing error in this case
				s.errorBatchHandler(view.ConsumerTag, view.Queue, batchSize, fmt.Errorf("processing failed: dropping batch: %w", err))
			} else {
				s.infoBatchHandler(view.ConsumerTag, view.Queue, batchSize, "processed batch")
			}
		} else {
			poolErr := s.ackBatchPostHandle(&view, lastDeliveryTag, batchSize, session, err)
			if poolErr != nil {
				return poolErr
			}
		}
	}
}

func (s *Subscriber) ackBatchPostHandle(view *BatchHandlerView, lastDeliveryTag uint64, currentBatchSize int, session *Session, handlerErr error) (err error) {
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
		s.warnBatchHandler(view.ConsumerTag, view.Queue, currentBatchSize, ackErr, "batch (n)ack failed")
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
		s.infoBatchHandler(view.ConsumerTag, view.Queue, currentBatchSize, "nacked batch")
	} else {
		s.infoBatchHandler(view.ConsumerTag, view.Queue, currentBatchSize, "acked batch")
	}
	// successfully handled message
	return nil
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
