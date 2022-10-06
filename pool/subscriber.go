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

type HandlerFunc func(amqp091.Delivery) error

type Handler struct {
	Queue    string
	Consumer string

	AutoAck   bool
	Exclusive bool
	NoLocal   bool
	NoWait    bool
	Args      amqp091.Table

	HandlerFunc HandlerFunc
}

func (s *Subscriber) RegisterHandlerFunc(queue string, consumer string, autoAck bool, exclusive bool, noLocal bool, noWait bool, args amqp091.Table, hf HandlerFunc) {
	if hf == nil {
		panic("HandlerFunc must not be nil")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.handlers = append(s.handlers, Handler{
		Queue:    queue,
		Consumer: consumer,

		AutoAck:   autoAck,
		Exclusive: exclusive,
		NoLocal:   noLocal,
		NoWait:    noWait,
		Args:      args,

		HandlerFunc: hf,
	})

	s.log.WithFields(map[string]any{
		"subscriber": s.pool.Name(),
		"consumer":   consumer,
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
		"consumer":   handler.Consumer,
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
	defer func() {
		// after starting everything we want to set started to true
		s.started = true

		s.log.WithField("subscriber", s.pool.Name()).Info("started subscribers.")
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
	session, err := s.pool.GetSession()
	if err != nil {
		return err
	}
	defer func() {
		if err == nil || errors.Is(err, ErrClosed) {
			// no error or graceful shutdown
			s.pool.ReturnSession(session, false)
		} else {
			// actual error
			s.pool.ReturnSession(session, true)
		}
	}()

	// got a working session
	delivery, err := session.Consume(h.Queue, h.Consumer, h.AutoAck, h.Exclusive, h.NoLocal, h.NoWait, h.Args)
	if err != nil {
		return err
	}

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
				s.info(msg.Exchange, msg.RoutingKey, h.Queue, "received message")
				err = h.HandlerFunc(msg)
				if h.AutoAck {
					// no acks required
					if err == nil {
						// TODO: potential message loss
						s.info(msg.Exchange, msg.RoutingKey, h.Queue, "processed message")
						break handle
					} else if errors.Is(err, ErrClosed) {
						// unknown error
						s.warn(msg.Exchange, msg.RoutingKey, h.Queue, fmt.Errorf("potential message loss: %w", err), string(msg.Body))
					} else {
						// unknown error
						s.error(msg.Exchange, msg.RoutingKey, h.Queue, fmt.Errorf("potential message loss: %w", err), string(msg.Body))
						break handle
					}
				}

			ack:
				for {
					if err != nil {
						ackErr = session.Nack(msg.DeliveryTag, false, true)
					} else {
						ackErr = session.Ack(msg.DeliveryTag, false)
					}

					if ackErr != nil {
						poolErr = session.Recover()
						if poolErr != nil {
							// only returns an error upon shutdown
							// TODO: potential message loss
							// transient session for shutdown?

							s.warn(msg.Exchange, msg.RoutingKey, h.Queue, fmt.Errorf("potential message loss: missing (n)ack: %w", err), string(msg.Body))
							return poolErr
						}
						// recovered successfully, retry ack/nack
						continue ack
					}

					if err != nil {
						s.info(msg.Exchange, msg.RoutingKey, h.Queue, "nacked message")
					} else {
						s.info(msg.Exchange, msg.RoutingKey, h.Queue, "acked message")
					}
					// successfully handled message
					break handle
				}
			}
		}
	}
}

func (s *Subscriber) catchShutdown() <-chan struct{} {
	return s.ctx.Done()
}

func (s *Subscriber) info(exchange, routingKey, queue string, a ...any) {
	s.log.WithFields(map[string]any{
		"subscriber": s.pool.Name(),
		"exchange":   exchange,
		"routingKey": routingKey,
		"queue":      queue,
	}).Info(a...)
}

func (s *Subscriber) warn(exchange, routingKey, queue string, err error, a ...any) {
	s.log.WithFields(map[string]any{
		"subscriber": s.pool.Name(),
		"exchange":   exchange,
		"routingKey": routingKey,
		"queue":      queue,
		"error":      err,
	}).Warn(a...)
}

func (s *Subscriber) error(exchange, routingKey, queue string, err error, a ...any) {
	s.log.WithFields(map[string]any{
		"subscriber": s.pool.Name(),
		"exchange":   exchange,
		"routingKey": routingKey,
		"queue":      queue,
		"error":      err,
	}).Error(a...)
}

func (s *Subscriber) debug(exchange, routingKey, queue string, a ...any) {
	s.log.WithFields(map[string]any{
		"subscriber": s.pool.Name(),
		"exchange":   exchange,
		"routingKey": routingKey,
		"queue":      queue,
	}).Debug(a...)
}
