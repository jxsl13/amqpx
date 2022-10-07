package amqpx

import (
	"fmt"
	"strings"
	"sync"

	"github.com/jxsl13/amqpx/pool"
	"github.com/rabbitmq/amqp091-go"
)

var (
	pubPool *pool.Pool
	pub     *pool.Publisher
	sub     *pool.Subscriber

	mu               sync.Mutex
	handlers         = make([]pool.Handler, 0)
	topologies       = make([]TopologyFunc, 0)
	topologyDeleters = make([]TopologyFunc, 0)

	startOnce sync.Once
	closeOnce sync.Once
)

type (
	Topologer    = pool.Topologer
	TopologyFunc func(*Topologer) error

	Delivery    = amqp091.Delivery
	HandlerFunc = func(Delivery) error

	Publishing = amqp091.Publishing
)

var (
	// QuorumArgs is the argument you need to pass in order to create a quorum queue.
	QuorumQueue = amqp091.Table{
		"x-queue-type": "quorum",
	}
)

const (
	// DeadLetterExchangeKey can be used in order to create a dead letter exchange
	// https://www.rabbitmq.com/dlx.html
	DeadLetterExchangeKey = "x-dead-letter-exchange"
)

// RegisterTopology registers a topology creating function that is called upon
// Start. The creation of topologie sis the first step before any publisher or subscriber is started.
func RegisterTopologyCreator(topology TopologyFunc) {
	if topology == nil {
		panic("topology must not be nil")
	}
	mu.Lock()
	defer mu.Unlock()

	topologies = append(topologies, topology)
}

// RegisterTopologyDeleter registers a topology finalizer that is executed at the end of
// amqpx.Close().
func RegisterTopologyDeleter(finalizer TopologyFunc) {
	if finalizer == nil {
		panic("topology must not be nil")
	}
	mu.Lock()
	defer mu.Unlock()

	// prepend, execution in reverse order
	topologyDeleters = append([]TopologyFunc{finalizer}, topologyDeleters...)
}

// RegisterHandler registers a handler function for a specific queue.
// consumer can be set to a unique consumer name (if left empty, a unique name will be generated)
func RegisterHandler(queue string, consumer string, autoAck bool, exclusive bool, noLocal bool, noWait bool, args amqp091.Table, handlerFunc HandlerFunc) {
	if handlerFunc == nil {
		panic("handlerFunc must not be nil")
	}

	mu.Lock()
	defer mu.Unlock()

	handlers = append(handlers, pool.Handler{
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

// NewURL creates a new conenction string for the NewSessionFactory
// hostname: 	e.g. localhost
// port:        e.g. 5672
// username:	e.g. username
// password:    e.g. password
// vhost:       e.g. "" or "/" or ""
func NewURL(hostname string, port int, username, password string, vhost ...string) string {
	vhoststr := ""
	if len(vhost) > 0 {
		vhoststr = strings.TrimLeft(vhost[0], "/")
	}

	return fmt.Sprintf("amqp://%s:%s@%s:%d/%s", username, password, hostname, port, vhoststr)
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
	startOnce.Do(func() {
		mu.Lock()
		defer mu.Unlock()

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
		pubPool, err = pool.New(
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
		if len(topologies) > 0 {
			// create topology
			topologer := pool.NewTopologer(pubPool)

			for _, t := range topologies {
				err = t(topologer)
				if err != nil {
					return
				}
			}

		}

		// publisher must before subscribers, as subscriber handlers might be using the publisher.
		// do NOT auto close publisher pool
		pub = pool.NewPublisher(pubPool, option.PublisherOptions...)

		// create subscriber pool in case handlers were registered
		if len(handlers) > 0 {
			sessions := len(handlers)
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
			sub = pool.NewSubscriber(subPool, pool.SubscriberWithAutoClosePool(true))
			for _, h := range handlers {
				sub.RegisterHandler(h)
			}
			sub.Start()
		}
	})
	return err
}

func Close() error {
	mu.Lock()
	defer mu.Unlock()

	return close()
}

func close() (err error) {
	closeOnce.Do(func() {

		if pub != nil {
			// close producers first
			pub.Close()
		}
		if sub != nil {
			sub.Close()
		}

		if pubPool != nil {
			topologer := pool.NewTopologer(pubPool)
			for _, f := range topologyDeleters {
				e := f(topologer)
				if e != nil {
					err = e
					return
				}
			}

			// finally close the publisher pool
			// which is also used for topology.
			pubPool.Close()
		}
	})
	return err
}

// Publish a message to a specific exchange with a given routingKey.
func Publish(exchange string, routingKey string, mandatory bool, immediate bool, msg Publishing) error {
	if pub == nil {
		panic("amqpx package was not started")
	}
	return pub.Publish(exchange, routingKey, mandatory, immediate, msg)
}

// Get is only supposed to be used for testing, do not use get for polling any broker queues.
func Get(queue string, autoAck bool) (msg *Delivery, ok bool, err error) {
	if pub == nil {
		panic("amqpx package was not started")
	}
	return pub.Get(queue, autoAck)
}

// Reset closes the current package and resets its state before it was initialized and started.
func Reset() {
	mu.Lock()
	defer mu.Unlock()
	close()

	pubPool = nil
	pub = nil
	sub = nil

	handlers = make([]pool.Handler, 0)
	topologies = make([]TopologyFunc, 0)
	topologyDeleters = make([]TopologyFunc, 0)

	startOnce = sync.Once{}
	closeOnce = sync.Once{}
}
