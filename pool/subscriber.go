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

// Handler is a struct that contains all parameters needed in order to register a handler function.
type Handler struct {
	Queue string
	ConsumeOptions
	HandlerFunc HandlerFunc

	session *Session
	running bool
}

// BatchHandler is a struct that contains all parameter sneeded i order to register a batch handler function.
type BatchHandler struct {
	Queue string

	// When <= 0, will be set to 50
	// Number of messages a batch may contain at most
	// before processing is triggered
	MaxBatchSize int

	// FlushTimeout is the duration that is waited for the next message from a queue before
	// the batch is closed and passed for processing.
	// This value should be less than 30m (which is the (n)ack timeout of RabbitMQ)
	// when <= 0, will be set to 5s
	FlushTimeout time.Duration
	ConsumeOptions
	HandlerFunc BatchHandlerFunc
}

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

	s.RegisterHandler(
		&Handler{
			Queue:          queue,
			ConsumeOptions: option,
			HandlerFunc:    hf,
		},
	)
}

func (s *Subscriber) RegisterHandler(handler *Handler) {
	if handler.HandlerFunc == nil {
		panic("handler.HandlerFunc must not be nil")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.handlers = append(s.handlers, handler)

	s.log.WithFields(withConsumerIfSet(handler.ConsumerTag,
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
func (s *Subscriber) RegisterBatchHandlerFunc(queue string, maxBatchSize int, flushTimeout time.Duration, hf BatchHandlerFunc, options ...ConsumeOptions) {

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

	s.RegisterBatchHandler(
		&BatchHandler{
			Queue:          queue,
			ConsumeOptions: option,
			MaxBatchSize:   maxBatchSize,
			FlushTimeout:   flushTimeout,
			HandlerFunc:    hf,
		},
	)
}

func (s *Subscriber) RegisterBatchHandler(handler *BatchHandler) {
	if handler.HandlerFunc == nil {
		panic("handler.HandlerFunc must not be nil")
	}

	// TODO: do we want to introduce a BatchSizeLimit
	// which would keep track of accumulated payload memory limits
	// of all the messages of a batch and process the messages
	// in case we hit that memory limit within the current batch.

	if handler.MaxBatchSize <= 0 {
		handler.MaxBatchSize = 50
	}

	if handler.FlushTimeout <= 0 {
		handler.FlushTimeout = 5 * time.Second
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.batchHandlers = append(s.batchHandlers, handler)

	s.log.WithFields(withConsumerIfSet(handler.ConsumerTag,
		map[string]any{
			"subscriber":   s.pool.Name(),
			"queue":        handler.Queue,
			"maxBatchSize": handler.MaxBatchSize,
			"flushTimeout": handler.FlushTimeout.String(),
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
	}
}

func (s *Subscriber) consume(h *Handler) (err error) {
	s.debugConsumer(h.ConsumerTag, "starting consumer...")

	session, err := s.pool.GetSession()
	if err != nil {
		return err
	}
	defer func() {
		if err == nil {
			// no error
			s.pool.ReturnSession(session, false)
			s.infoConsumer(h.ConsumerTag, "closed")
		} else if errors.Is(err, ErrClosed) {
			// graceful shutdown
			s.pool.ReturnSession(session, false)
			s.infoConsumer(h.ConsumerTag, "closed")
		} else {
			// actual error
			s.pool.ReturnSession(session, true)
			s.warnConsumer(h.ConsumerTag, err, "closed unexpectedly")
		}
	}()

	// got a working session
	delivery, err := session.Consume(
		h.Queue,
		h.ConsumeOptions,
	)
	if err != nil {
		return err
	}
	s.infoConsumer(h.ConsumerTag, "started")
	for {
		select {
		case <-s.catchShutdown():
			return ErrClosed
		case msg, ok := <-delivery:
			if !ok {
				return ErrDeliveryClosed
			}

			s.infoHandler(h.ConsumerTag, msg.Exchange, msg.RoutingKey, h.Queue, "received message")
			err = h.HandlerFunc(msg)
			if h.AutoAck {
				if err != nil {
					// we cannot really do anything to recover from a processing error in this case
					s.errorHandler(h.ConsumerTag, msg.Exchange, msg.RoutingKey, h.Queue, fmt.Errorf("processing failed: dropping message: %w", err))
				} else {
					s.infoHandler(h.ConsumerTag, msg.Exchange, msg.RoutingKey, h.Queue, "processed message")
				}
			} else {
				poolErr := s.ackPostHandle(h, msg.DeliveryTag, msg.Exchange, msg.RoutingKey, session, err)
				if poolErr != nil {
					return poolErr
				}
			}
		}
	}
}

// (n)ack delivery and signal that message was processed by the service
func (s *Subscriber) ackPostHandle(h *Handler, deliveryTag uint64, exchange, routingKey string, session *Session, handlerErr error) (err error) {
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
		s.warnHandler(h.ConsumerTag, exchange, routingKey, h.Queue, ackErr, "(n)ack failed")
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
		s.infoHandler(h.ConsumerTag, exchange, routingKey, h.Queue, "nacked message")
	} else {
		s.infoHandler(h.ConsumerTag, exchange, routingKey, h.Queue, "acked message")
	}
	// successfully handled message
	return nil
}

func (s *Subscriber) batchConsume(bh *BatchHandler) (err error) {
	s.debugConsumer(bh.ConsumerTag, "starting batch consumer...")

	session, err := s.pool.GetSession()
	if err != nil {
		return err
	}
	defer func() {
		if err == nil {
			// no error
			s.pool.ReturnSession(session, false)
			s.infoConsumer(bh.ConsumerTag, "closed")
		} else if errors.Is(err, ErrClosed) {
			// graceful shutdown
			s.pool.ReturnSession(session, false)
			s.infoConsumer(bh.ConsumerTag, "closed")
		} else {
			// actual error
			s.pool.ReturnSession(session, true)
			s.warnConsumer(bh.ConsumerTag, err, "closed unexpectedly")
		}
	}()

	// got a working session
	delivery, err := session.Consume(
		bh.Queue,
		bh.ConsumeOptions,
	)
	if err != nil {
		return err
	}
	s.infoConsumer(bh.ConsumerTag, "started")

	// preallocate memory for batch
	batch := make([]amqp091.Delivery, 0, bh.MaxBatchSize)
	defer func() {
		if len(batch) > 0 && errors.Is(err, ErrClosed) {
			// requeue all not yet processed messages in batch slice
			// we can only nack these message sin case the session has not been closed
			// Its does not make sense to use session.Nack at this point because in case the sessison dies
			// the delivery tags also die with it.
			// There is no way to recover form this state in case an error is returned from the Nack call.
			nackErr := batch[len(batch)-1].Nack(true, true)
			if nackErr != nil {
				s.warnBatchHandler(bh.ConsumerTag, bh.Queue, bh.MaxBatchSize, err, "failed to nack and requeue batch upon shutdown")
			}
		}
	}()

	var (
		timer   = time.NewTimer(bh.FlushTimeout)
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
			resetTimer(timer, bh.FlushTimeout, &drained)

			select {
			case <-s.catchShutdown():
				return ErrClosed
			case msg, ok := <-delivery:
				if !ok {
					return ErrDeliveryClosed
				}
				batch = append(batch, msg)
				if len(batch) == bh.MaxBatchSize {
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

		s.infoBatchHandler(bh.ConsumerTag, bh.Queue, batchSize, "received batch")
		err = bh.HandlerFunc(batch)
		// no acks required
		if bh.AutoAck {
			if err != nil {
				// we cannot really do anything to recover from a processing error in this case
				s.errorBatchHandler(bh.ConsumerTag, bh.Queue, batchSize, fmt.Errorf("processing failed: dropping batch: %w", err))
			} else {
				s.infoBatchHandler(bh.ConsumerTag, bh.Queue, batchSize, "processed batch")
			}
		} else {
			poolErr := s.ackBatchPostHandle(bh, lastDeliveryTag, batchSize, session, err)
			if poolErr != nil {
				return poolErr
			}
		}
	}
}

func (s *Subscriber) ackBatchPostHandle(bh *BatchHandler, lastDeliveryTag uint64, currentBatchSize int, session *Session, handlerErr error) (err error) {
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
		s.warnBatchHandler(bh.ConsumerTag, bh.Queue, currentBatchSize, ackErr, "batch (n)ack failed")
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
		s.infoBatchHandler(bh.ConsumerTag, bh.Queue, currentBatchSize, "nacked batch")
	} else {
		s.infoBatchHandler(bh.ConsumerTag, bh.Queue, currentBatchSize, "acked batch")
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
