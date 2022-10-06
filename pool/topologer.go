package pool

import (
	"github.com/jxsl13/amqpx/logging"
	"github.com/rabbitmq/amqp091-go"
)

type Topologer struct {
	pool *Pool

	log logging.Logger
}

func NewTopologer(p *Pool, options ...TopologerOption) *Topologer {
	if p == nil {
		panic("nil pool passed")
	}

	option := topologerOption{
		Logger: p.sp.log, // derive logger from session pool
	}

	for _, o := range options {
		o(&option)
	}

	top := &Topologer{
		pool: p,
		log:  option.Logger,
	}
	return top
}

// ExchangeDeclare declares an exchange on the server. If the exchange does not
// already exist, the server will create it.  If the exchange exists, the server
// verifies that it is of the provided type, durability and auto-delete flags.
//
// Errors returned from this method will close the channel.
//
// Exchange names starting with "amq." are reserved for pre-declared and
// standardized exchanges. The client MAY declare an exchange starting with
// "amq." if the passive option is set, or the exchange already exists.  Names can
// consist of a non-empty sequence of letters, digits, hyphen, underscore,
// period, or colon.
//
// Each exchange belongs to one of a set of exchange kinds/types implemented by
// the server. The exchange types define the functionality of the exchange - i.e.
// how messages are routed through it. Once an exchange is declared, its type
// cannot be changed.  The common types are "direct", "fanout", "topic" and
// "headers".
//
// Durable and Non-Auto-Deleted exchanges will survive server restarts and remain
// declared when there are no remaining bindings.  This is the best lifetime for
// long-lived exchange configurations like stable routes and default exchanges.
//
// Non-Durable and Auto-Deleted exchanges will be deleted when there are no
// remaining bindings and not restored on server restart.  This lifetime is
// useful for temporary topologies that should not pollute the virtual host on
// failure or after the consumers have completed.
//
// Non-Durable and Non-Auto-deleted exchanges will remain as long as the server is
// running including when there are no remaining bindings.  This is useful for
// temporary topologies that may have long delays between bindings.
//
// Durable and Auto-Deleted exchanges will survive server restarts and will be
// removed before and after server restarts when there are no remaining bindings.
// These exchanges are useful for robust temporary topologies or when you require
// binding durable queues to auto-deleted exchanges.
//
// Note: RabbitMQ declares the default exchange types like 'amq.fanout' as
// durable, so queues that bind to these pre-declared exchanges must also be
// durable.
//
// Exchanges declared as `internal` do not accept accept publishings. Internal
// exchanges are useful when you wish to implement inter-exchange topologies
// that should not be exposed to users of the broker.
//
// When noWait is true, declare without waiting for a confirmation from the server.
// The channel may be closed as a result of an error.  Add a NotifyClose listener
// to respond to any exceptions.
//
// Optional amqp.Table of arguments that are specific to the server's implementation of
// the exchange can be sent for exchange types that require extra parameters.
func (t *Topologer) ExchangeDeclare(name string, kind string, durable bool, autoDelete bool, internal bool, noWait bool, args amqp091.Table) (err error) {
	s, err := t.pool.GetSession()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			t.pool.ReturnSession(s, true)
		} else {
			t.pool.ReturnSession(s, false)
		}
	}()
	return s.ExchangeDeclare(name, kind, durable, autoDelete, internal, noWait, args)
}

// ExchangeDelete removes the named exchange from the server. When an exchange is
// deleted all queue bindings on the exchange are also deleted.  If this exchange
// does not exist, the channel will be closed with an error.
//
// When ifUnused is true, the server will only delete the exchange if it has no queue
// bindings.  If the exchange has queue bindings the server does not delete it
// but close the channel with an exception instead.  Set this to true if you are
// not the sole owner of the exchange.
//
// When noWait is true, do not wait for a server confirmation that the exchange has
// been deleted.  Failing to delete the channel could close the channel.  Add a
// NotifyClose listener to respond to these channel exceptions.
func (t *Topologer) ExchangeDelete(name string, ifUnused bool, noWait bool) (err error) {
	s, err := t.pool.GetSession()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			t.pool.ReturnSession(s, true)
		} else {
			t.pool.ReturnSession(s, false)
		}
	}()

	return s.ExchangeDelete(name, ifUnused, noWait)
}

// QueueDeclare declares a queue to hold messages and deliver to consumers.
// Declaring creates a queue if it doesn't already exist, or ensures that an
// existing queue matches the same parameters.
//
// Every queue declared gets a default binding to the empty exchange "" which has
// the type "direct" with the routing key matching the queue's name.  With this
// default binding, it is possible to publish messages that route directly to
// this queue by publishing to "" with the routing key of the queue name.
//
//	QueueDeclare("alerts", true, false, false, false, nil)
//	Publish("", "alerts", false, false, Publishing{Body: []byte("...")})
//
//	Delivery       Exchange  Key       Queue
//	-----------------------------------------------
//	key: alerts -> ""     -> alerts -> alerts
//
// The queue name may be empty, in which case the server will generate a unique name
// which will be returned in the Name field of Queue struct.
//
// Durable and Non-Auto-Deleted queues will survive server restarts and remain
// when there are no remaining consumers or bindings.  Persistent publishings will
// be restored in this queue on server restart.  These queues are only able to be
// bound to durable exchanges.
//
// Non-Durable and Auto-Deleted queues will not be redeclared on server restart
// and will be deleted by the server after a short time when the last consumer is
// canceled or the last consumer's channel is closed.  Queues with this lifetime
// can also be deleted normally with QueueDelete.  These durable queues can only
// be bound to non-durable exchanges.
//
// Non-Durable and Non-Auto-Deleted queues will remain declared as long as the
// server is running regardless of how many consumers.  This lifetime is useful
// for temporary topologies that may have long delays between consumer activity.
// These queues can only be bound to non-durable exchanges.
//
// Durable and Auto-Deleted queues will be restored on server restart, but without
// active consumers will not survive and be removed.  This Lifetime is unlikely
// to be useful.
//
// Exclusive queues are only accessible by the connection that declares them and
// will be deleted when the connection closes.  Channels on other connections
// will receive an error when attempting  to declare, bind, consume, purge or
// delete a queue with the same name.
//
// When noWait is true, the queue will assume to be declared on the server.  A
// channel exception will arrive if the conditions are met for existing queues
// or attempting to modify an existing queue from a different connection.
//
// When the error return value is not nil, you can assume the queue could not be
// declared with these parameters, and the channel will be closed.
func (t *Topologer) QueueDeclare(name string, durable bool, autoDelete bool, exclusive bool, noWait bool, args amqp091.Table) (err error) {
	s, err := t.pool.GetSession()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			t.pool.ReturnSession(s, true)
		} else {
			t.pool.ReturnSession(s, false)
		}
	}()
	return s.QueueDeclare(name, durable, autoDelete, exclusive, noWait, args)
}

// QueueDelete removes the queue from the server including all bindings then
// purges the messages based on server configuration, returning the number of
// messages purged.
//
// When ifUnused is true, the queue will not be deleted if there are any
// consumers on the queue.  If there are consumers, an error will be returned and
// the channel will be closed.
//
// When ifEmpty is true, the queue will not be deleted if there are any messages
// remaining on the queue.  If there are messages, an error will be returned and
// the channel will be closed.
//
// When noWait is true, the queue will be deleted without waiting for a response
// from the server.  The purged message count will not be meaningful. If the queue
// could not be deleted, a channel exception will be raised and the channel will
// be closed.
func (t *Topologer) QueueDelete(name string, ifUnused bool, ifEmpty bool, noWait bool) (i int, err error) {
	s, err := t.pool.GetSession()
	if err != nil {
		return 0, err
	}
	defer func() {
		if err != nil {
			t.pool.ReturnSession(s, true)
		} else {
			t.pool.ReturnSession(s, false)
		}
	}()
	return s.QueueDelete(name, ifUnused, ifEmpty, noWait)
}

// QueueBind binds an exchange to a queue so that publishings to the exchange will
// be routed to the queue when the publishing routing key matches the binding
// routing key.
//
//	QueueBind("pagers", "alert", "log", false, nil)
//	QueueBind("emails", "info", "log", false, nil)
//
//	Delivery       Exchange  Key       Queue
//	-----------------------------------------------
//	key: alert --> log ----> alert --> pagers
//	key: info ---> log ----> info ---> emails
//	key: debug --> log       (none)    (dropped)
//
// If a binding with the same key and arguments already exists between the
// exchange and queue, the attempt to rebind will be ignored and the existing
// binding will be retained.
//
// In the case that multiple bindings may cause the message to be routed to the
// same queue, the server will only route the publishing once.  This is possible
// with topic exchanges.
//
//	QueueBind("pagers", "alert", "amq.topic", false, nil)
//	QueueBind("emails", "info", "amq.topic", false, nil)
//	QueueBind("emails", "#", "amq.topic", false, nil) // match everything
//
//	Delivery       Exchange        Key       Queue
//	-----------------------------------------------
//	key: alert --> amq.topic ----> alert --> pagers
//	key: info ---> amq.topic ----> # ------> emails
//	                         \---> info ---/
//	key: debug --> amq.topic ----> # ------> emails
//
// It is only possible to bind a durable queue to a durable exchange regardless of
// whether the queue or exchange is auto-deleted.  Bindings between durable queues
// and exchanges will also be restored on server restart.
//
// If the binding could not complete, an error will be returned and the channel
// will be closed.
//
// When noWait is false and the queue could not be bound, the channel will be
// closed with an error.
func (t *Topologer) QueueBind(name string, routingKey string, exchange string, noWait bool, args amqp091.Table) (err error) {
	s, err := t.pool.GetSession()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			t.pool.ReturnSession(s, true)
		} else {
			t.pool.ReturnSession(s, false)
		}
	}()
	return s.QueueBind(name, routingKey, exchange, noWait, args)
}

// QueueUnbind removes a binding between an exchange and queue matching the key and
// arguments.

// It is possible to send and empty string for the exchange name which means to
// unbind the queue from the default exchange.
func (t *Topologer) QueueUnbind(name string, routingKey string, exchange string, args amqp091.Table) (err error) {
	s, err := t.pool.GetSession()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			t.pool.ReturnSession(s, true)
		} else {
			t.pool.ReturnSession(s, false)
		}
	}()
	return s.QueueUnbind(name, routingKey, exchange, args)
}

// ExchangeBind binds an exchange to another exchange to create inter-exchange
// routing topologies on the server.  This can decouple the private topology and
// routing exchanges from exchanges intended solely for publishing endpoints.
//
// Binding two exchanges with identical arguments will not create duplicate
// bindings.
//
// Binding one exchange to another with multiple bindings will only deliver a
// message once.  For example if you bind your exchange to `amq.fanout` with two
// different binding keys, only a single message will be delivered to your
// exchange even though multiple bindings will match.
//
// Given a message delivered to the source exchange, the message will be forwarded
// to the destination exchange when the routing key is matched.
//
//	ExchangeBind("sell", "MSFT", "trade", false, nil)
//	ExchangeBind("buy", "AAPL", "trade", false, nil)
//
//	Delivery       Source      Key      Destination
//	example        exchange             exchange
//	-----------------------------------------------
//	key: AAPL  --> trade ----> MSFT     sell
//	                     \---> AAPL --> buy
//
// When noWait is true, do not wait for the server to confirm the binding.  If any
// error occurs the channel will be closed.  Add a listener to NotifyClose to
// handle these errors.
//
// Optional arguments specific to the exchanges bound can also be specified.
func (t *Topologer) ExchangeBind(destination string, routingKey string, source string, noWait bool, args amqp091.Table) (err error) {
	s, err := t.pool.GetSession()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			t.pool.ReturnSession(s, true)
		} else {
			t.pool.ReturnSession(s, false)
		}
	}()
	return s.ExchangeBind(destination, routingKey, source, noWait, args)
}

// ExchangeUnbind unbinds the destination exchange from the source exchange on the
// server by removing the routing key between them.  This is the inverse of
// ExchangeBind.  If the binding does not currently exist, an error will be
// returned.
//
// When noWait is true, do not wait for the server to confirm the deletion of the
// binding.  If any error occurs the channel will be closed.  Add a listener to
// NotifyClose to handle these errors.
//
// Optional arguments that are specific to the type of exchanges bound can also be
// provided.  These must match the same arguments specified in ExchangeBind to
// identify the binding.
func (t *Topologer) ExchangeUnbind(destination string, routingKey string, source string, noWait bool, args amqp091.Table) (err error) {
	s, err := t.pool.GetSession()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			t.pool.ReturnSession(s, true)
		} else {
			t.pool.ReturnSession(s, false)
		}
	}()

	return s.ExchangeUnbind(destination, routingKey, source, noWait, args)
}
