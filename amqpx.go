package amqpx

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jxsl13/amqpx/pool"
)

var (
	// global variable, as we usually only need a single connection
	amqpx = New()
)

type (
	TopologyFunc func(context.Context, *pool.Topologer) error
)

func noopCancel() {}

type AMQPX struct {
	pubCtx    context.Context
	pubCancel context.CancelFunc
	pubPool   *pool.Pool
	pub       *pool.Publisher
	sub       *pool.Subscriber

	mu            sync.RWMutex
	handlers      []*pool.Handler
	batchHandlers []*pool.BatchHandler

	topologies       []TopologyFunc
	topologyDeleters []TopologyFunc

	closeTimeout time.Duration

	startOnce sync.Once
	closeOnce sync.Once
}

func New() *AMQPX {
	return &AMQPX{
		pubCtx:    context.Background(),
		pubCancel: noopCancel,
		pubPool:   nil,
		pub:       nil,
		sub:       nil,

		handlers:         make([]*pool.Handler, 0),
		batchHandlers:    make([]*pool.BatchHandler, 0),
		topologies:       make([]TopologyFunc, 0),
		topologyDeleters: make([]TopologyFunc, 0),
	}
}

// Reset closes the current package and resets its state before it was initialized and started.
func (a *AMQPX) Reset() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	err := a.close()

	a.pubCtx = context.Background()
	a.pubCancel = noopCancel
	a.pubPool = nil
	a.pub = nil
	a.sub = nil

	a.handlers = make([]*pool.Handler, 0)
	a.batchHandlers = make([]*pool.BatchHandler, 0)

	a.topologies = make([]TopologyFunc, 0)
	a.topologyDeleters = make([]TopologyFunc, 0)

	a.startOnce = sync.Once{}
	a.closeOnce = sync.Once{}
	return err
}

// NewURL creates a new connection string for the NewSessionFactory
// hostname: 	e.g. localhost
// port:        e.g. 5672
// username:	e.g. username
// password:    e.g. password
// vhost:       e.g. "" or "/"
func NewURL(hostname string, port int, username, password string, vhost ...string) string {
	vhoststr := "/"
	if len(vhost) > 0 {
		first := vhost[0]
		if strings.HasPrefix(first, "/") {
			vhoststr = first
		} else {
			vhoststr = "/" + first
		}
	}

	return fmt.Sprintf("amqp://%s:%s@%s:%d%s", username, password, hostname, port, vhoststr)
}

// RegisterTopology registers a topology creating function that is called upon
// Start. The creation of topologie sis the first step before any publisher or subscriber is started.
func (a *AMQPX) RegisterTopologyCreator(topology TopologyFunc) {
	if topology == nil {
		panic("topology must not be nil")
	}
	a.mu.Lock()
	defer a.mu.Unlock()

	a.topologies = append(a.topologies, topology)
}

// RegisterTopologyDeleter registers a topology finalizer that is executed at the end of
// amqpx.Close().
func (a *AMQPX) RegisterTopologyDeleter(finalizer TopologyFunc) {
	if finalizer == nil {
		panic("topology must not be nil")
	}
	a.mu.Lock()
	defer a.mu.Unlock()

	// prepend, execution in reverse order
	a.topologyDeleters = append([]TopologyFunc{finalizer}, a.topologyDeleters...)
}

// RegisterHandler registers a handler function for a specific queue.
// consumer can be set to a unique consumer name (if left empty, a unique name will be generated)
func (a *AMQPX) RegisterHandler(queue string, handlerFunc pool.HandlerFunc, option ...pool.ConsumeOptions) *pool.Handler {
	a.mu.Lock()
	defer a.mu.Unlock()

	o := pool.ConsumeOptions{}
	if len(option) > 0 {
		o = option[0]
	}

	handler := pool.NewHandler(queue, handlerFunc, o)
	a.handlers = append(a.handlers, handler)
	return handler
}

// RegisterBatchHandler registers a handler function for a specific queue that processes batches.
// consumer can be set to a unique consumer name (if left empty, a unique name will be generated)
func (a *AMQPX) RegisterBatchHandler(queue string, handlerFunc pool.BatchHandlerFunc, option ...pool.BatchHandlerOption) *pool.BatchHandler {
	//  maxBatchSize int, flushTimeout time.Duration,
	if handlerFunc == nil {
		panic("handlerFunc must not be nil")
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	handler := pool.NewBatchHandler(queue, handlerFunc, option...)
	a.batchHandlers = append(a.batchHandlers, handler)
	return handler
}

// Start starts the subscriber and publisher pools.
// In case no handlers were registered, no subscriber pool will be started.
// connectUrl has the form: amqp://username:password@localhost:5672
// pubSessions is the number of pooled sessions (channels) for the publisher.
// options are optional pool connection options. They might also contain publisher specific
// settings like publish confirmations or a custom context which can signal an application shutdown.
// This customcontext does not replace the Close() call. Always defer a Close() call.
// Start is a non-blocking operation.
func (a *AMQPX) Start(ctx context.Context, connectUrl string, options ...Option) (err error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.startOnce.Do(func() {

		// sane defaults
		option := option{
			PoolOptions:           make([]pool.Option, 0),
			PublisherOptions:      make([]pool.PublisherOption, 0),
			PublisherConnections:  1,
			PublisherSessions:     10,
			SubscriberConnections: len(a.handlers) + len(a.batchHandlers),
			CloseTimeout:          15 * time.Second,
		}

		for _, o := range options {
			o(&option)
		}

		// affects the topology deleter when close is called
		// which stops deleting or reconnecting after the timeout
		a.closeTimeout = option.CloseTimeout

		pubPoolOptions := make([]pool.Option, 0, 2+len(option.PoolOptions))
		pubPoolOptions = append(pubPoolOptions,
			pool.WithNameSuffix("-pub"),
			pool.WithConfirms(true), // the more secure option in order to prevent message loss
		)

		// allow the user to overwrite previous options
		pubPoolOptions = append(pubPoolOptions, option.PoolOptions...)

		// publisher and subscriber need to have different tcp connections (tcp pushback prevention)
		// pub pool is only closed when .Close() is called.
		// This context.Background() is needed so that we can correctly call the topology deleters.
		a.pubCtx, a.pubCancel = context.WithCancel(context.Background())
		a.pubPool, err = pool.New(
			a.pubCtx,
			connectUrl,
			option.PublisherConnections,
			option.PublisherSessions,
			pubPoolOptions..., // allow to overwrite defaults
		)
		if err != nil {
			return
		}

		// use publisher pool for topology
		if len(a.topologies) > 0 {
			// create topology
			topologer := pool.NewTopologer(a.pubPool)

			for _, t := range a.topologies {
				err = t(ctx, topologer)
				if err != nil {
					return
				}
			}
		}

		// publisher must before subscribers, as subscriber handlers might be using the publisher.
		// do NOT auto close publisher pool
		a.pub = pool.NewPublisher(a.pubPool, option.PublisherOptions...)

		// create subscriber pool in case handlers were registered
		requiredHandlers := len(a.handlers) + len(a.batchHandlers)
		if requiredHandlers > 0 {
			var (
				sessions    = requiredHandlers
				connections = requiredHandlers
			)

			if 0 < option.SubscriberConnections && option.SubscriberConnections <= connections {
				// you may decrease the number of tcp connections
				// between one connection with N sessions or N connections with one session each
				connections = option.SubscriberConnections
			}

			subPoolOptions := make([]pool.Option, 0, 1+len(option.PoolOptions))

			// overwritable options
			subPoolOptions = append(subPoolOptions, pool.WithNameSuffix("-sub"))

			// overwriting or additional options
			subPoolOptions = append(subPoolOptions, option.PoolOptions...)

			// subscriber needs as many channels as there are handler functions
			// because we do not want subscriber connections to interfere
			// with each other
			var subPool *pool.Pool
			subPool, err = pool.New(
				ctx,
				connectUrl,
				connections,       // one connection per handler
				sessions,          // one session per handler
				subPoolOptions..., // allow the user to overwrite the defaults EXCEPT for the confirms setting
			)
			if err != nil {
				return
			}
			a.sub = pool.NewSubscriber(
				subPool,
				pool.SubscriberWithAutoClosePool(true), // pass pool ownership to subscriber
			)
			for _, h := range a.handlers {
				a.sub.RegisterHandler(h)
			}
			for _, bh := range a.batchHandlers {
				a.sub.RegisterBatchHandler(bh)
			}
			err = a.sub.Start(ctx)
			if err != nil {
				return
			}
		}
	})
	return err
}

func (a *AMQPX) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.close()
}

func (a *AMQPX) close() (err error) {
	a.closeOnce.Do(func() {
		defer a.pubCancel()

		if a.sub != nil {
			a.sub.Close()
		}

		if a.pub != nil {
			a.pub.Close()
		}

		if a.pubPool != nil && len(a.topologyDeleters) > 0 {
			ctx, cancel := context.WithTimeout(a.pubCtx, a.closeTimeout)
			defer cancel()

			topologer := pool.NewTopologer(
				a.pubPool,
				pool.TopologerWithContext(ctx),
				pool.TopologerWithTransientSessions(true),
			)
			for _, f := range a.topologyDeleters {
				err = errors.Join(err, f(ctx, topologer))
			}
		}

		if a.pubPool != nil {
			// finally close the publisher pool
			// which is also used for topology.
			a.pubPool.Close()
		}
	})
	return err
}

// Publish a message to a specific exchange with a given routingKey.
// You may set exchange to "" and routingKey to your queue name in order to publish directly to a queue.
func (a *AMQPX) Publish(ctx context.Context, exchange string, routingKey string, msg pool.Publishing) error {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.pub == nil {
		panic("amqpx package was not started")
	}

	return a.pub.Publish(ctx, exchange, routingKey, msg)
}

// Get is only supposed to be used for testing, do not use get for polling any broker queues.
func (a *AMQPX) Get(ctx context.Context, queue string, autoAck bool) (msg pool.Delivery, ok bool, err error) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.pub == nil {
		panic("amqpx package was not started")
	}

	// publisher is used because this is a testing method for the publisher
	return a.pub.Get(ctx, queue, autoAck)
}

// RegisterTopology registers a topology creating function that is called upon
// Start. The creation of topologie sis the first step before any publisher or subscriber is started.
func RegisterTopologyCreator(topology TopologyFunc) {
	amqpx.RegisterTopologyCreator(topology)
}

// RegisterTopologyDeleter registers a topology finalizer that is executed at the end of
// amqpx.Close().
func RegisterTopologyDeleter(finalizer TopologyFunc) {
	amqpx.RegisterTopologyDeleter(finalizer)
}

// RegisterHandler registers a handler function for a specific queue.
// consumer can be set to a unique consumer name (if left empty, a unique name will be generated)
// The returned handler can be used to pause message processing and resume paused processing.
// The processing must have been started with Start before it can be paused or resumed.
func RegisterHandler(queue string, handlerFunc pool.HandlerFunc, option ...pool.ConsumeOptions) *pool.Handler {
	return amqpx.RegisterHandler(queue, handlerFunc, option...)
}

// RegisterBatchHandler registers a handler function for a specific queue that processes batches.
// consumer can be set to a unique consumer name (if left empty, a unique name will be generated)
func RegisterBatchHandler(queue string, handlerFunc pool.BatchHandlerFunc, option ...pool.BatchHandlerOption) *pool.BatchHandler {
	return amqpx.RegisterBatchHandler(queue, handlerFunc, option...)
}

// Start starts the subscriber and publisher pools.
// In case no handlers were registered, no subscriber pool will be started.
// connectUrl has the form: amqp://username:password@localhost:5672
// pubSessions is the number of pooled sessions (channels) for the publisher.
// options are optional pool connection options. They might also contain publisher specific
// settings like publish confirmations or a custom context which can signal an application shutdown.
// This customcontext does not replace the Close() call. Always defer a Close() call.
// Start is a non-blocking operation.
// The startup context may differ from the cancelation context provided via the options.
func Start(ctx context.Context, connectUrl string, options ...Option) (err error) {
	return amqpx.Start(ctx, connectUrl, options...)
}

func Close() error {
	return amqpx.Close()
}

// Publish a message to a specific exchange with a given routingKey.
// You may set exchange to "" and routingKey to your queue name in order to publish directly to a queue.
func Publish(ctx context.Context, exchange string, routingKey string, msg pool.Publishing) error {
	return amqpx.Publish(ctx, exchange, routingKey, msg)
}

// Get is only supposed to be used for testing, do not use get for polling any broker queues.
func Get(ctx context.Context, queue string, autoAck bool) (msg pool.Delivery, ok bool, err error) {
	return amqpx.Get(ctx, queue, autoAck)
}

// Reset closes the current package and resets its state before it was initialized and started.
func Reset() error {
	return amqpx.Reset()
}
