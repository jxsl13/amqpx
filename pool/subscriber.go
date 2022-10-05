package pool

import (
	"context"
	"errors"
	"sync"
)

type Subscriber struct {
	pool          *Pool
	autoClosePool bool

	mu       sync.Mutex
	started  bool
	handlers []handler

	ctx    context.Context
	cancel context.CancelFunc

	wg sync.WaitGroup
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
	}

	return sub
}

type HandlerFunc func(Delivery) error

type handler struct {
	Queue    string
	Consumer string

	AutoAck   bool
	Exclusive bool
	NoLocal   bool
	NoWait    bool
	Args      Table

	HandlerFunc HandlerFunc
}

func (s *Subscriber) RegisterHandler(queue string, consumer string, autoAck bool, exclusive bool, noLocal bool, noWait bool, args Table, hf HandlerFunc) {
	if hf == nil {
		panic("HandlerFunc must not be nil")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.handlers = append(s.handlers, handler{
		Queue:    queue,
		Consumer: consumer,

		AutoAck:   autoAck,
		Exclusive: exclusive,
		NoLocal:   noLocal,
		NoWait:    noWait,
		Args:      args,

		HandlerFunc: hf,
	})
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

	}()

	for _, h := range s.handlers {
		s.wg.Add(1)
		go s.consumer(h, &s.wg)
	}
}

func (s *Subscriber) consumer(h handler, wg *sync.WaitGroup) {
	defer wg.Done()

	var err error
	for {
		err = s.consume(h)
		if errors.Is(err, ErrClosed) {
			return
		}
	}
}

func (s *Subscriber) consume(h handler) (err error) {
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
				err = h.HandlerFunc(msg)
				if err == nil || errors.Is(err, ErrClosed) {
					break handle
				}

				if h.AutoAck {
					// no acks required in this case
					break handle
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
							return poolErr
						}
					}
					break ack
				}
			}
		}
	}
}

func (s *Subscriber) catchShutdown() <-chan struct{} {
	return s.ctx.Done()
}
