package amqpx

import (
	"sync"

	"github.com/jxsl13/amqpx/pool"
)

var (
	pub *pool.Publisher
	sub *pool.Subscriber

	mu       sync.Mutex
	handlers = make([]pool.Handler, 0)

	startOnce sync.Once
	closeOnce sync.Once
)

// RegisterHandler registers a handler function for a specific queue.
// consumer can be set to a unique consumer name (if left empty, a unique name will be generated)
func RegisterHandler(queue string, consumer string, autoAck bool, exclusive bool, noLocal bool, noWait bool, args pool.Table, handlerFunc pool.HandlerFunc) {
	mu.Lock()
	defer mu.Unlock()

	if handlerFunc == nil {
		panic("handlerFunc must not be nil")
	}

	handlers = append(handlers, pool.Handler{
		Queue:    queue,
		Consumer: consumer,
		AutoAck:  autoAck,
	})
}

type TopologyFunc func(*pool.Topologer) error

// Start starts the subscriber and publisher pools.
// In case no handlers were registered, no subscriber pool will be started.
// connectUrl has the form: amqp://username:password@localhost:5672
// pubSessions is the number of pooled sessions (channels) for the publisher.
// options are optional pool connection options. They might also contain publisher specific
// settings like publish confirmations or a custom context which can signal an application shutdown.
// This customcontext does not replace the Close() call. Always defer a Close() call.
// Start is a non-blocking operation.
func Start(connectUrl string, topology TopologyFunc, options ...Option) (err error) {
	startOnce.Do(func() {
		mu.Lock()
		defer mu.Unlock()

		// sane defaults
		option := option{
			PoolOptions:           make([]pool.PoolOption, 0),
			PublisherOptions:      make([]pool.PublisherOption, 0),
			PublisherConnections:  1,
			PublisherSessions:     10,
			SubscriberConnections: 1,
		}

		for _, o := range options {
			o(&option)
		}

		// publisher and subscriber need to have different tcp connections (tcp pushback prevention)
		pubPool, err := pool.New(
			connectUrl,
			option.PublisherConnections,
			option.PublisherSessions,
			append(option.PoolOptions, pool.WithNameSuffix("-pub"))...,
		)
		if err != nil {
			return
		}

		// use publisher pool for topology
		if topology != nil {
			// create topology
			topologer := pool.NewTopologer(pubPool)
			err = topology(topologer)
			if err != nil {
				return
			}
		}

		pub = pool.NewPublisher(pubPool, append(option.PublisherOptions, pool.PublisherWithAutoClosePool(true))...)

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
				append(option.PoolOptions, pool.WithNameSuffix("-sub"))...,
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

func Close() {
	closeOnce.Do(func() {
		// close producers first
		pub.Close()
		sub.Close()
	})
}

// Publish a message to a specific exchange with a given routingKey.
func Publish(exchange string, routingKey string, mandatory bool, immediate bool, msg pool.Publishing) error {
	return pub.Publish(exchange, routingKey, mandatory, immediate, msg)
}
