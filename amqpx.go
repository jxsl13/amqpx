package amqpx

import (
	"fmt"
	"strings"
	"sync"

	"github.com/jxsl13/amqpx/pool"
	"github.com/rabbitmq/amqp091-go"
)

var (
	// global variable, as we usually only need a single connection
	amqpx = New()
)

type (
	Table = amqp091.Table

	Topologer    = pool.Topologer
	TopologyFunc func(*Topologer) error

	Delivery    = amqp091.Delivery
	HandlerFunc = func(Delivery) error

	Publishing = amqp091.Publishing
)

var (
	// QuorumArgs is the argument you need to pass in order to create a quorum queue.
	QuorumQueue = Table{
		"x-queue-type": "quorum",
	}
)

const (
	// DeadLetterExchangeKey can be used in order to create a dead letter exchange
	// https://www.rabbitmq.com/dlx.html
	DeadLetterExchangeKey = "x-dead-letter-exchange"
)

type AMQPX struct {
	pubPool *pool.Pool
	pub     *pool.Publisher
	sub     *pool.Subscriber

	mu               sync.Mutex
	handlers         []pool.Handler
	topologies       []TopologyFunc
	topologyDeleters []TopologyFunc

	startOnce sync.Once
	closeOnce sync.Once
}

func New() *AMQPX {
	return &AMQPX{
		pubPool: nil,
		pub:     nil,
		sub:     nil,

		handlers:         make([]pool.Handler, 0),
		topologies:       make([]TopologyFunc, 0),
		topologyDeleters: make([]TopologyFunc, 0),
	}
}

// NewURL creates a new connection string for the NewSessionFactory
// hostname: 	e.g. localhost
// port:        e.g. 5672
// username:	e.g. username
// password:    e.g. password
// vhost:       e.g. "" or "/"
func NewURL(hostname string, port int, username, password string, vhost ...string) string {
	vhoststr := ""
	if len(vhost) > 0 {
		vhoststr = strings.TrimLeft(vhost[0], "/")
	}

	return fmt.Sprintf("amqp://%s:%s@%s:%d/%s", username, password, hostname, port, vhoststr)
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
func (a *AMQPX) RegisterHandler(queue string, consumer string, autoAck bool, exclusive bool, noLocal bool, noWait bool, args Table, handlerFunc HandlerFunc) {
	if handlerFunc == nil {
		panic("handlerFunc must not be nil")
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	a.handlers = append(a.handlers, pool.Handler{
		Queue:       queue,
		Consumer:    consumer,
		AutoAck:     autoAck,
		Exclusive:   exclusive,
		NoLocal:     noLocal,
		NoWait:      noWait,
		Args:        args,
		HandlerFunc: handlerFunc,
	})
}

// Start starts the subscriber and publisher pools.
// In case no handlers were registered, no subscriber pool will be started.
// connectUrl has the form: amqp://username:password@localhost:5672
// pubSessions is the number of pooled sessions (channels) for the publisher.
// options are optional pool connection options. They might also contain publisher specific
// settings like publish confirmations or a custom context which can signal an application shutdown.
// This customcontext does not replace the Close() call. Always defer a Close() call.
// Start is a non-blocking operation.
func (a *AMQPX) Start(connectUrl string, options ...Option) (err error) {
	a.startOnce.Do(func() {
		a.mu.Lock()
		defer a.mu.Unlock()

		// sane defaults
		option := option{
			PoolOptions:           make([]pool.Option, 0),
			PublisherOptions:      make([]pool.PublisherOption, 0),
			PublisherConnections:  1,
			PublisherSessions:     10,
			SubscriberConnections: 1,
		}

		for _, o := range options {
			o(&option)
		}

		// publisher and subscriber need to have different tcp connections (tcp pushback prevention)
		a.pubPool, err = pool.New(
			connectUrl,
			option.PublisherConnections,
			option.PublisherSessions,
			append([]pool.Option{
				pool.WithNameSuffix("-pub"),
				pool.WithConfirms(true),
			}, option.PoolOptions...)..., // allow to overwrite defaults
		)
		if err != nil {
			return
		}

		// use publisher pool for topology
		if len(a.topologies) > 0 {
			// create topology
			topologer := pool.NewTopologer(a.pubPool)

			for _, t := range a.topologies {
				err = t(topologer)
				if err != nil {
					return
				}
			}

		}

		// publisher must before subscribers, as subscriber handlers might be using the publisher.
		// do NOT auto close publisher pool
		a.pub = pool.NewPublisher(a.pubPool, option.PublisherOptions...)

		// create subscriber pool in case handlers were registered
		if len(a.handlers) > 0 {
			sessions := len(a.handlers)
			connections := option.SubscriberConnections
			if connections < 0 || sessions < connections {
				connections = sessions
			}

			// subscriber needs as many channels as there are handler functions
			var subPool *pool.Pool
			subPool, err = pool.New(
				connectUrl,
				connections,
				sessions,
				append([]pool.Option{
					pool.WithNameSuffix("-sub"),
				}, option.PoolOptions...)..., // allow the user to overwrite the defaults.
			)
			if err != nil {
				return
			}
			a.sub = pool.NewSubscriber(subPool, pool.SubscriberWithAutoClosePool(true))
			for _, h := range a.handlers {
				a.sub.RegisterHandler(h)
			}
			a.sub.Start()
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

		if a.pub != nil {
			// close producers first
			a.pub.Close()
		}
		if a.sub != nil {
			a.sub.Close()
		}

		if a.pubPool != nil {
			topologer := pool.NewTopologer(a.pubPool)
			for _, f := range a.topologyDeleters {
				e := f(topologer)
				if e != nil {
					err = e
					return
				}
			}

			// finally close the publisher pool
			// which is also used for topology.
			a.pubPool.Close()
		}
	})
	return err
}

// Publish a message to a specific exchange with a given routingKey.
func (a *AMQPX) Publish(exchange string, routingKey string, mandatory bool, immediate bool, msg Publishing) error {
	if a.pub == nil {
		panic("amqpx package was not started")
	}
	return a.pub.Publish(exchange, routingKey, mandatory, immediate, msg)
}

// Get is only supposed to be used for testing, do not use get for polling any broker queues.
func (a *AMQPX) Get(queue string, autoAck bool) (msg *Delivery, ok bool, err error) {
	if a.pub == nil {
		panic("amqpx package was not started")
	}
	return a.pub.Get(queue, autoAck)
}

// Reset closes the current package and resets its state before it was initialized and started.
func (a *AMQPX) Reset() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.close()

	a.pubPool = nil
	a.pub = nil
	a.sub = nil

	a.handlers = make([]pool.Handler, 0)
	a.topologies = make([]TopologyFunc, 0)
	a.topologyDeleters = make([]TopologyFunc, 0)

	a.startOnce = sync.Once{}
	a.closeOnce = sync.Once{}
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
func RegisterHandler(queue string, consumer string, autoAck bool, exclusive bool, noLocal bool, noWait bool, args Table, handlerFunc HandlerFunc) {
	amqpx.RegisterHandler(queue, consumer, autoAck, exclusive, noLocal, noWait, args, handlerFunc)
}

// Start starts the subscriber and publisher pools.
// In case no handlers were registered, no subscriber pool will be started.
// connectUrl has the form: amqp://username:password@localhost:5672
// pubSessions is the number of pooled sessions (channels) for the publisher.
// options are optional pool connection options. They might also contain publisher specific
// settings like publish confirmations or a custom context which can signal an application shutdown.
// This customcontext does not replace the Close() call. Always defer a Close() call.
// Start is a non-blocking operation.
func Start(connectUrl string, options ...Option) (err error) {
	return amqpx.Start(connectUrl, options...)
}

func Close() error {
	return amqpx.Close()
}

// Publish a message to a specific exchange with a given routingKey.
func Publish(exchange string, routingKey string, mandatory bool, immediate bool, msg Publishing) error {
	return amqpx.Publish(exchange, routingKey, mandatory, immediate, msg)
}

// Get is only supposed to be used for testing, do not use get for polling any broker queues.
func Get(queue string, autoAck bool) (msg *Delivery, ok bool, err error) {
	return amqpx.Get(queue, autoAck)
}

// Reset closes the current package and resets its state before it was initialized and started.
func Reset() {
	amqpx.Reset()
}
