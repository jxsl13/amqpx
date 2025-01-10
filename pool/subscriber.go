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
	ctx, cc := context.WithCancelCause(option.Ctx)
	cancel := toCancelFunc(fmt.Errorf("subscriber %w", ErrClosed), cc)

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
type HandlerFunc func(context.Context, Delivery) error

// BatchHandlerFunc is a handler for incoming batches of messages/events
type BatchHandlerFunc func(context.Context, []Delivery) error

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

	s.mu.Lock()
	defer s.mu.Unlock()
	s.batchHandlers = append(s.batchHandlers, handler)

	opts := handler.Config()

	s.log.WithFields(withConsumerIfSet(handler.ConsumeOptions().ConsumerTag,
		map[string]any{
			"subscriber":    s.pool.Name(),
			"queue":         opts.Queue,
			"maxBatchBytes": opts.MaxBatchBytes,
			"maxBatchSize":  opts.MaxBatchSize,
			"flushTimeout":  opts.FlushTimeout,
		})).Info("registered batch message handler")
}

// Start starts the consumers for all registered handler functions
// This method is not blocking. Use Wait() to wait for all routines to shut down
// via context cancelation (e.g. via a signal)
func (s *Subscriber) Start(ctx context.Context) (err error) {
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
	s.wg.Add(len(s.handlers))
	for _, h := range s.handlers {
		go s.consumer(h, &s.wg)
		err = h.awaitResumed(ctx)
		if err != nil {
			return fmt.Errorf("failed to start consumer for queue %s: %w", h.Queue(), err)
		}
	}

	s.debugSimple(fmt.Sprintf("starting %d batch handler routine(s)", len(s.batchHandlers)))
	s.wg.Add(len(s.batchHandlers))
	for _, bh := range s.batchHandlers {
		go s.batchConsumer(bh, &s.wg)

		// wait for consumer to have started.
		err = bh.awaitResumed(ctx)
		if err != nil {
			return fmt.Errorf("failed to start batch consumer for queue %s: %w", bh.Queue(), err)
		}
	}
	return nil
}

func (s *Subscriber) retry(consumerType string, h handler, f func() (err error)) {
	var (
		backoff = newDefaultBackoffPolicy(1*time.Millisecond, 5*time.Second)
		retry   = 0
		err     error
		timer   = time.NewTimer(0)
		drained = false
	)
	defer closeTimer(timer, &drained)

	for {
		opts := h.QueueConfig()

		err = f()
		// only return if closed
		if errors.Is(err, ErrClosed) {
			return
		}
		// consume was canceled due to context being closed (paused)
		if errors.Is(err, errPausingCancel) {
			s.infoConsumer(opts.ConsumerTag, fmt.Sprintf("%s paused: pausing %v", consumerType, err))
			continue
		}

		if errors.Is(err, ErrDeliveryClosed) {
			select {
			case <-h.pausing().Done():
				s.infoConsumer(opts.ConsumerTag, fmt.Sprintf("%s paused: %v", consumerType, err))
				continue
			default:
				// not paused
			}
		}

		s.error(opts.ConsumerTag, opts.Queue, err, fmt.Sprintf("%s closed unexpectedly: %v", consumerType, err))

		retry++
		resetTimer(timer, backoff(retry), &drained)

		select {
		case <-s.catchShutdown():
			return
		case <-timer.C:
			// at this point we know that the timer channel has been drained
			drained = true
			continue
		}
	}
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

	s.retry("consumer", h, func() error {
		select {
		case <-s.catchShutdown():
			return s.shutdownErr()
		case <-h.resuming().Done():
			return s.consume(h)
		}
	})
}

func (s *Subscriber) consume(h *Handler) (err error) {
	opts, err := h.start(s.ctx)
	if err != nil {
		return err
	}
	defer h.paused()

	s.debugConsumer(opts.ConsumerTag, "starting consumer...")

	session, err := s.pool.GetSession(s.ctx)
	if err != nil {
		return err
	}
	defer func() {
		// err evaluation upon defer
		s.returnSession(h, session, err)
	}()

	// got a working session
	delivery, err := session.ConsumeWithContext(
		h.pausing(),
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
			return s.shutdownErr()
		case msg, ok := <-delivery:
			if !ok {
				return ErrDeliveryClosed
			}

			s.infoHandler(opts.ConsumerTag, msg.Exchange, msg.RoutingKey, opts.Queue, "received message")
			err = opts.HandlerFunc(h.pausing(), msg)
			if opts.AutoAck {
				if err != nil {
					// we cannot really do anything to recover from a processing error in this case
					s.errorHandler(opts.ConsumerTag, msg.Exchange, msg.RoutingKey, opts.Queue, fmt.Errorf("processing failed: dropping message: %w", err))
				} else {
					s.infoHandler(opts.ConsumerTag, msg.Exchange, msg.RoutingKey, opts.Queue, "processed message")
				}
			} else {
				poolErr := s.postProcessing(opts, msg.DeliveryTag, session, err)
				if poolErr != nil {
					return poolErr
				}
			}
		}
	}
}

// (n)ack delivery and signal that message was processed by the service
func (s *Subscriber) postProcessing(opts HandlerConfig, deliveryTag uint64, session *Session, handlerErr error) (err error) {
	if handlerErr == nil {
		err = session.Ack(deliveryTag, false)
		if err != nil {
			// cannot do anything at this point
			return fmt.Errorf("failed to ack message: %w", err)
		}
		s.infoConsumer(opts.ConsumerTag, "acked message")
		return nil
	} else if errors.Is(handlerErr, ErrReject) {
		err = session.Nack(deliveryTag, false, false)
		if err != nil {
			// cannot do anything at this point
			return fmt.Errorf("failed to reject message: %w", err)
		}
		s.infoConsumer(opts.ConsumerTag, "rejected message")
		return nil
	}
	// requeue message if possible
	err = session.Nack(deliveryTag, false, true)
	if err != nil {

		return fmt.Errorf("failed to requeue message: %w", err)
	}
	s.infoConsumer(opts.ConsumerTag, "requeued message")
	return nil
}

func (s *Subscriber) batchConsumer(h *BatchHandler, wg *sync.WaitGroup) {
	defer wg.Done()
	defer h.close()

	// initialize all handler contexts
	// to be in state resuming
	// INFO: h.start is intentionally called twice, here and in the consumer!
	opts, err := h.start(s.ctx)
	if err != nil {
		s.error(opts.ConsumerTag, opts.Queue, err, "failed to start batch handler consumer")
		return
	}

	s.retry("batch consumer", h, func() error {
		select {
		case <-s.catchShutdown():
			return s.shutdownErr()
		case <-h.resuming().Done():
			return s.batchConsume(h)
		}
	})
}

func (s *Subscriber) batchConsume(h *BatchHandler) (err error) {
	// INFO: h.start is intentionally called twice, because we migh have a changed
	// handler after pausing the processing.
	opts, err := h.start(s.ctx) // initialize all contexts to be in state resuming with parent context s.ctx
	if err != nil {
		return err
	}
	defer h.paused()

	s.debugConsumer(opts.ConsumerTag, "starting batch consumer...")

	session, err := s.pool.GetSession(s.ctx)
	if err != nil {
		return err
	}
	defer func() {
		// err evaluation upon defer
		s.returnSession(h, session, err)
	}()

	if !opts.ConsumeOptions.AutoAck {
		// INFO: currently setting prefetch_size != 0 is not supported by RabbitMQ
		// which is why we only set the prefetch_count here.
		// batch size after which rabbtmq can expect a nack or ack
		err = session.Qos(h.pausing(), opts.MaxBatchSize, 0)
		if err != nil {
			return fmt.Errorf("failed to set QoS for batch consumer: %w", err)
		}
	}

	// got a working session
	delivery, err := session.ConsumeWithContext(
		h.pausing(),
		opts.Queue,
		opts.ConsumeOptions,
	)
	if err != nil {
		return err
	}

	h.resumed() // the connection was established successfully at this point, the caller of resume may proceed his execution.
	s.infoConsumer(opts.ConsumerTag, "started")

	// preallocate memory for batch
	batch := make([]Delivery, 0, maxi(1, opts.MaxBatchSize))
	defer func() {
		if errors.Is(err, ErrClosed) {
			// requeue all not yet processed messages in batch slice
			// we can only nack these messages in case the session has not been closed
			// Its does not make sense to use session.Nack at this point because in case that the sessison dies
			// the delivery tags also die with it.
			// There is no way to recover form this state in case an error is returned from the Nack call.
			reqErr := s.requeueBatch(opts, session, batch)
			if reqErr != nil {
				s.errorBatchHandler(opts.ConsumerTag,
					opts.Queue,
					opts.MaxBatchSize,
					opts.MaxBatchBytes,
					fmt.Errorf("failed to requeue batch upon shutdown: %w", reqErr),
				)
			} else {
				s.infoBatchHandlerf(
					opts.ConsumerTag,
					opts.Queue,
					opts.MaxBatchSize,
					opts.MaxBatchBytes,
					"requeued %d batch messages upon shutdown",
					len(batch),
				)
			}

		} else if errors.Is(err, ErrDeliveryClosed) && isContextClosed(h.pausing()) {
			reqErr := s.requeueBatch(opts, session, batch)
			if reqErr != nil {
				s.errorBatchHandler(opts.ConsumerTag,
					opts.Queue,
					opts.MaxBatchSize,
					opts.MaxBatchBytes,
					fmt.Errorf("failed to requeue batch upon pausing: %w", reqErr),
				)
			} else {
				s.infoBatchHandlerf(
					opts.ConsumerTag,
					opts.Queue,
					opts.MaxBatchSize,
					opts.MaxBatchBytes,
					"requeued %d batch messages upon pausing",
					len(batch),
				)
			}
		}

	}()

	var (
		timer      = time.NewTimer(opts.FlushTimeout)
		drained    = false
		batchBytes = 0
	)
	defer closeTimer(timer, &drained)

	for {
		// reset batch slice
		// reuse memory
		batch = batch[:0]
		batchBytes = 0

	collectBatch:
		for {

			// reset the timer
			resetTimer(timer, opts.FlushTimeout, &drained)

			select {
			case <-s.catchShutdown():
				return s.shutdownErr()
			case msg, ok := <-delivery:
				if !ok {
					return ErrDeliveryClosed
				}

				batchBytes += len(msg.Body)
				batch = append(batch, msg)
				if opts.MaxBatchSize > 0 && len(batch) == opts.MaxBatchSize {
					break collectBatch
				}

				if opts.MaxBatchBytes > 0 && batchBytes >= opts.MaxBatchBytes {
					break collectBatch
				}

			case <-timer.C:
				// at this point we know that the timer channel has been drained
				drained = true
				if len(batch) > 0 {
					// timeout reached, process batch that might not contain
					// a full batch, yet.
					s.infoBatchHandler(
						opts.ConsumerTag,
						opts.Queue,
						len(batch),
						batchBytes,
						"flush timeout reached",
					)
					break collectBatch
				}

				// no new messages, continue, reset timer and try again
			}
		}

		// at this point we have a batch to work with
		var (
			batchSize = len(batch)
		)

		s.infoBatchHandler(opts.ConsumerTag, opts.Queue, batchSize, batchBytes, "received batch")
		err = opts.HandlerFunc(h.pausing(), batch)
		if opts.AutoAck {
			// no acks required
			if err != nil {
				// we cannot really do anything to recover from a processing error in this case
				s.errorBatchHandler(
					opts.ConsumerTag,
					opts.Queue,
					batchSize,
					batchBytes,
					fmt.Errorf("processing failed: dropping batch: %w", err),
				)
			} else {
				s.infoBatchHandler(
					opts.ConsumerTag,
					opts.Queue,
					batchSize,
					batchBytes,
					"processed batch",
				)
			}
		} else {
			poolErr := s.batchPostProcessing(opts, session, batch, err)
			if poolErr != nil {
				return poolErr
			}
		}
	}
}

func (s *Subscriber) ackBatch(opts BatchHandlerConfig, session *Session, batch []Delivery) (err error) {
	batchSize := len(batch)
	if batchSize == 0 {
		return nil
	}

	defer func() {
		if err != nil {
			err = fmt.Errorf("failed to ack batch of %d messages: %w", batchSize, err)
		}
	}()

	lastDeliveryTag := batch[batchSize-1].DeliveryTag
	if batchSize == opts.MaxBatchSize {
		err = session.Ack(lastDeliveryTag, true)
		if err != nil {
			// cannot do anything at this point
			return fmt.Errorf("failed to ack full batch: %w", err)
		}
		s.infofConsumer(opts.ConsumerTag, "acked full batch with delivery tag %d", lastDeliveryTag)
		return nil
	}

	err = session.Ack(lastDeliveryTag, true)
	if err != nil {
		return fmt.Errorf("failed to ack partial batch: %w", err)
	}
	s.infofConsumer(opts.ConsumerTag, "ackeded partial batch of %d messages with delivery tag %d", batchSize, lastDeliveryTag)

	return nil
}

func (s *Subscriber) requeueBatch(opts BatchHandlerConfig, session *Session, batch []Delivery) error {
	return s.nackBatch(opts, session, batch, true)
}

func (s *Subscriber) rejectBatch(opts BatchHandlerConfig, session *Session, batch []Delivery) error {
	return s.nackBatch(opts, session, batch, false)
}

// we expect that at this point the prefetch_count of Qos is set to the MaxBatchSize,
// which is why once we change the Qos setting, we have to reset it back to the previous value
// batchBytes is only used for logging purposes
// This requeue works with the assumption that it is not possible to (n)ack messages of partial batches whose size is smaller than the prefetch_count without
// changing the prefetch_count of the Qos setting.
func (s *Subscriber) nackBatch(opts BatchHandlerConfig, session *Session, batch []Delivery, requeue bool) (err error) {
	batchSize := len(batch)
	if batchSize == 0 {
		return
	}

	mode := "reject"
	if requeue {
		mode = "requeue"
	}

	defer func() {
		if err != nil {
			err = fmt.Errorf("failed to %s batch of %d messages: %w", mode, batchSize, err)
		}
	}()

	lastDeliveryTag := batch[batchSize-1].DeliveryTag
	if batchSize == opts.MaxBatchSize {
		err = session.Nack(lastDeliveryTag, true, true)
		if err != nil {
			// cannot do anything at this point
			return fmt.Errorf("failed to nack full batch: %w", err)
		}
		s.infoConsumer(opts.ConsumerTag, "nacked full batch")
		return nil
	}

	// nack & requeue
	err = session.Nack(lastDeliveryTag, true, true)
	if err != nil {
		return fmt.Errorf("failed to %s partial batch: %w", mode, err)
	}
	s.infofConsumer(opts.ConsumerTag, "%sed partial batch of %d messages", mode, batchSize)

	return nil
}

func isContextClosed(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

func (s *Subscriber) batchPostProcessing(opts BatchHandlerConfig, session *Session, batch []Delivery, handlerErr error) (err error) {

	// processing failed
	if handlerErr == nil {
		return s.ackBatch(opts, session, batch)
	} else if errors.Is(handlerErr, ErrReject) {
		// reject multiple
		return s.rejectBatch(opts, session, batch)
	}
	// requeue message if possible & nack all previous messages
	return s.requeueBatch(opts, session, batch)
}

type handler interface {
	QueueConfig() QueueConfig
	pausing() context.Context
}

func (s *Subscriber) returnSession(h handler, session *Session, err error) {
	opts := h.QueueConfig()

	if errors.Is(err, ErrClosed) {
		// graceful shutdown
		s.pool.ReturnSession(session, err)
		s.infoConsumer(opts.ConsumerTag, "closed")
		return
	}

	select {
	case <-h.pausing().Done():
		// expected closing due to context cancelation
		// cancel errors the underlying channel
		// A canceled session is an erred session.
		s.pool.ReturnSession(session, h.pausing().Err())
		s.infoConsumer(opts.ConsumerTag, "paused")
	default:
		// actual error
		s.pool.ReturnSession(session, err)
		s.warnConsumer(opts.ConsumerTag, err, "closed unexpectedly")
	}
}

func (s *Subscriber) catchShutdown() <-chan struct{} {
	return s.ctx.Done()
}

func (s *Subscriber) shutdownErr() error {
	return s.ctx.Err()
}

func (s *Subscriber) infoBatchHandler(consumer, queue string, batchSize, batchBytes int, a ...any) {
	s.log.WithFields(withConsumerIfSet(consumer,
		map[string]any{
			"batchSize":  batchSize,
			"batchBytes": batchBytes,
			"subscriber": s.pool.Name(),
			"queue":      queue,
		})).Info(a...)
}

func (s *Subscriber) infoBatchHandlerf(consumer, queue string, batchSize, batchBytes int, msg string, a ...any) {
	s.log.WithFields(withConsumerIfSet(consumer,
		map[string]any{
			"batchSize":  batchSize,
			"batchBytes": batchBytes,
			"subscriber": s.pool.Name(),
			"queue":      queue,
		})).Infof(msg, a...)
}

func (s *Subscriber) warnBatchHandler(consumer, queue string, batchSize, batchBytes int, err error, a ...any) {
	s.log.WithFields(withConsumerIfSet(consumer, map[string]any{
		"batchSize":  batchSize,
		"batchBytes": batchBytes,
		"subscriber": s.pool.Name(),
		"queue":      queue,
		"error":      err,
	})).Warn(a...)
}

func (s *Subscriber) warnBatchHandlerf(consumer, queue string, batchSize, batchBytes int, err error, msg string, a ...any) {
	s.log.WithFields(withConsumerIfSet(consumer, map[string]any{
		"batchSize":  batchSize,
		"batchBytes": batchBytes,
		"subscriber": s.pool.Name(),
		"queue":      queue,
		"error":      err,
	})).Warnf(msg, a...)
}

func (s *Subscriber) errorBatchHandler(consumer, queue string, batchSize, batchBytes int, err error, a ...any) {
	s.log.WithFields(withConsumerIfSet(consumer,
		map[string]any{
			"batchSize":  batchSize,
			"batchBytes": batchBytes,
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

func (s *Subscriber) debugfConsumer(consumer string, format string, a ...any) {
	s.log.WithFields(withConsumerIfSet(consumer, map[string]any{
		"subscriber": s.pool.Name(),
	})).Debugf(format, a...)
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

func (s *Subscriber) infofConsumer(consumer string, format string, a ...any) {
	s.log.WithFields(withConsumerIfSet(consumer, map[string]any{
		"subscriber": s.pool.Name(),
	})).Infof(format, a...)
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
