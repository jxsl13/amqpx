package pool

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/jxsl13/amqpx/logging"
	"github.com/rabbitmq/amqp091-go"
)

type Subscriber struct {
	pool          *Pool
	autoClosePool bool

	mu       sync.Mutex
	started  bool
	handlers []Handler

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

// Handler is a struct that contains all parameters needed in order to register a handler function.
type Handler struct {
	Queue string
	ConsumeOptions
	HandlerFunc HandlerFunc
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
	if hf == nil {
		panic("HandlerFunc must not be nil")
	}

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

	s.mu.Lock()
	defer s.mu.Unlock()
	s.handlers = append(s.handlers, Handler{
		Queue:          queue,
		ConsumeOptions: option,

		HandlerFunc: hf,
	})

	s.log.WithFields(map[string]any{
		"subscriber": s.pool.Name(),
		"consumer":   option.ConsumerTag,
		"queue":      queue,
	}).Info("registered handler")
}

func (s *Subscriber) RegisterHandler(handler Handler) {
	if handler.HandlerFunc == nil {
		panic("handler.HandlerFunc must not be nil")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.handlers = append(s.handlers, handler)

	s.log.WithFields(map[string]any{
		"subscriber": s.pool.Name(),
		"consumer":   handler.ConsumerTag,
		"queue":      handler.Queue,
	}).Info("registered handler")
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

	for _, h := range s.handlers {
		s.wg.Add(1)
		go s.consumer(h, &s.wg)
	}
}

func (s *Subscriber) consumer(h Handler, wg *sync.WaitGroup) {
	defer wg.Done()

	var err error
	for {
		err = s.consume(h)
		if errors.Is(err, ErrClosed) {
			return
		}
	}
}

func (s *Subscriber) consume(h Handler) (err error) {
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

			var (
				err     error
				ackErr  error
				poolErr error
			)
		handle:
			for {
				s.infoHandler(h.ConsumerTag, msg.Exchange, msg.RoutingKey, h.Queue, "received message")
				err = h.HandlerFunc(msg)
				if h.AutoAck {
					// no acks required
					if err == nil {
						s.infoHandler(h.ConsumerTag, msg.Exchange, msg.RoutingKey, h.Queue, "processed message")
						break handle
					} else if errors.Is(err, ErrClosed) {
						// unknown error
						s.warnHandler(h.ConsumerTag, msg.Exchange, msg.RoutingKey, h.Queue, fmt.Errorf("potential message loss: %w", err))
						return err
					} else {
						// unknown error -> recover & retry
						poolErr = session.Recover()
						if poolErr != nil {
							// only returns an error upon shutdown
							s.errorHandler(h.ConsumerTag, msg.Exchange, msg.RoutingKey, h.Queue, fmt.Errorf("potential message loss: %w", err))
							return poolErr
						}
						continue handle
					}
				}

				// processing failed
				if err != nil {
					// requeue message if possible
					ackErr = session.Nack(msg.DeliveryTag, false, true)
				} else {
					ackErr = session.Ack(msg.DeliveryTag, false)
				}

				// if (n)ack fails, we know that the connection died
				// potentially before when processing already.
				if ackErr != nil {
					s.warnHandler(h.ConsumerTag, msg.Exchange, msg.RoutingKey, h.Queue, ackErr, "(n)ack failed")
					poolErr = session.Recover()
					if poolErr != nil {
						// only returns an error upon shutdown
						return poolErr
					}
					// recovered successfully, retry ack/nack
					// do not retry, because th ebroker will requeue the un(n)acked message
					// after a timeout of by default 30 minutes.
					break handle
				}

				// (n)acked successfully
				if err != nil {
					s.infoHandler(h.ConsumerTag, msg.Exchange, msg.RoutingKey, h.Queue, "nacked message")
				} else {
					s.infoHandler(h.ConsumerTag, msg.Exchange, msg.RoutingKey, h.Queue, "acked message")
				}
				// successfully handled message
				break handle

			}
		}
	}
}

func (s *Subscriber) catchShutdown() <-chan struct{} {
	return s.ctx.Done()
}

func (s *Subscriber) infoHandler(consumer, exchange, routingKey, queue string, a ...any) {
	s.log.WithFields(map[string]any{
		"subscriber": s.pool.Name(),
		"consumer":   consumer,
		"exchange":   exchange,
		"routingKey": routingKey,
		"queue":      queue,
	}).Info(a...)
}

func (s *Subscriber) warnHandler(consumer, exchange, routingKey, queue string, err error, a ...any) {
	s.log.WithFields(map[string]any{
		"subscriber": s.pool.Name(),
		"consumer":   consumer,
		"exchange":   exchange,
		"routingKey": routingKey,
		"queue":      queue,
		"error":      err,
	}).Warn(a...)
}

func (s *Subscriber) errorHandler(consumer, exchange, routingKey, queue string, err error, a ...any) {
	s.log.WithFields(map[string]any{
		"subscriber": s.pool.Name(),
		"consumer":   consumer,
		"exchange":   exchange,
		"routingKey": routingKey,
		"queue":      queue,
		"error":      err,
	}).Error(a...)
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
	s.log.WithFields(map[string]any{
		"subscriber": s.pool.Name(),
		"consumer":   consumer,
	}).Debug(a...)
}

func (s *Subscriber) warnConsumer(consumer string, err error, a ...any) {
	s.log.WithFields(map[string]any{
		"subscriber": s.pool.Name(),
		"consumer":   consumer,
		"error":      err,
	}).Warn(a...)
}

func (s *Subscriber) infoConsumer(consumer string, a ...any) {
	s.log.WithFields(map[string]any{
		"subscriber": s.pool.Name(),
		"consumer":   consumer,
	}).Info(a...)
}
