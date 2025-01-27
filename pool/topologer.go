package pool

import (
	"context"

	"github.com/jxsl13/amqpx/logging"
	"github.com/jxsl13/amqpx/types"
)

type Topologer struct {
	pool *Pool

	transientOnly bool
	log           logging.Logger
	ctx           context.Context
}

func NewTopologer(p *Pool, options ...TopologerOption) *Topologer {
	if p == nil {
		panic("nil pool passed")
	}

	option := topologerOption{
		Logger: p.sp.log, // derive logger from session pool
		Ctx:    p.Context(),
	}

	for _, o := range options {
		o(&option)
	}

	top := &Topologer{
		pool: p,
		log:  option.Logger,
		ctx:  option.Ctx,
	}
	return top
}

func (t *Topologer) getSession(ctx context.Context) (*types.Session, error) {

	if t.transientOnly || t.pool.SessionPoolSize() == 0 {
		return t.pool.GetTransientSession(ctx)
	}
	return t.pool.GetSession(ctx)
}

// ExchangeDeclare declares an exchange on the server. If the exchange does not
// already exist, the server will create it.  If the exchange exists, the server
// verifies that it is of the provided type, durability and auto-delete flags.
//
// Errors returned from this method will close the session.
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
func (t *Topologer) ExchangeDeclare(ctx context.Context, name string, kind types.ExchangeKind, option ...types.ExchangeDeclareOptions) (err error) {
	s, err := t.getSession(ctx)
	if err != nil {
		return err
	}
	defer func() {
		t.pool.ReturnSession(s, err)
	}()
	return s.ExchangeDeclare(ctx, name, kind, option...)
}

// ExchangeDeclarePassive is functionally and parametrically equivalent to
// ExchangeDeclare, except that it sets the "passive" attribute to true. A passive
// exchange is assumed by RabbitMQ to already exist, and attempting to connect to a
// non-existent exchange will cause RabbitMQ to throw an exception. This function
// can be used to detect the existence of an exchange.
func (t *Topologer) ExchangeDeclarePassive(ctx context.Context, name string, kind types.ExchangeKind, option ...types.ExchangeDeclareOptions) (err error) {
	s, err := t.getSession(ctx)
	if err != nil {
		return err
	}
	defer func() {
		t.pool.ReturnSession(s, err)
	}()
	return s.ExchangeDeclarePassive(ctx, name, kind, option...)
}

// ExchangeDelete removes the named exchange from the server. When an exchange is
// deleted all queue bindings on the exchange are also deleted.  If this exchange
// does not exist, the channel will be closed with an error.
func (t *Topologer) ExchangeDelete(ctx context.Context, name string, option ...types.ExchangeDeleteOptions) (err error) {
	s, err := t.getSession(ctx)
	if err != nil {
		return err
	}
	defer func() {
		t.pool.ReturnSession(s, err)
	}()

	return s.ExchangeDelete(ctx, name, option...)
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
func (t *Topologer) QueueDeclare(ctx context.Context, name string, option ...types.QueueDeclareOptions) (queue types.Queue, err error) {
	s, err := t.getSession(ctx)
	if err != nil {
		return types.Queue{}, err
	}
	defer func() {
		t.pool.ReturnSession(s, err)
	}()
	return s.QueueDeclare(ctx, name, option...)
}

// QueueDeclarePassive is functionally and parametrically equivalent to QueueDeclare, except that it sets the "passive" attribute to true.
// A passive queue is assumed by RabbitMQ to already exist, and attempting to connect to a non-existent queue will cause RabbitMQ to throw an exception.
// This function can be used to test for the existence of a queue.
func (t *Topologer) QueueDeclarePassive(ctx context.Context, name string, option ...types.QueueDeclareOptions) (queue types.Queue, err error) {
	s, err := t.getSession(ctx)
	if err != nil {
		return types.Queue{}, err
	}
	defer func() {
		t.pool.ReturnSession(s, err)
	}()
	return s.QueueDeclarePassive(ctx, name, option...)
}

// QueuePurge removes all messages from the named queue which are not waiting to be acknowledged.
// Messages that have been delivered but have not yet been acknowledged will not be removed.
// When successful, returns the number of messages purged.
func (t *Topologer) QueuePurge(ctx context.Context, name string, options ...types.QueuePurgeOptions) (int, error) {
	s, err := t.getSession(ctx)
	if err != nil {
		return 0, err
	}
	defer func() {
		t.pool.ReturnSession(s, err)
	}()
	return s.QueuePurge(ctx, name, options...)
}

// QueueDelete removes the queue from the server including all bindings then
// purges the messages based on server configuration, returning the number of
// messages purged.
func (t *Topologer) QueueDelete(ctx context.Context, name string, option ...types.QueueDeleteOptions) (purged int, err error) {
	s, err := t.getSession(ctx)
	if err != nil {
		return 0, err
	}
	defer func() {
		t.pool.ReturnSession(s, err)
	}()
	return s.QueueDelete(ctx, name, option...)
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
func (t *Topologer) QueueBind(ctx context.Context, name string, routingKey string, exchange string, option ...types.QueueBindOptions) (err error) {
	s, err := t.getSession(ctx)
	if err != nil {
		return err
	}
	defer func() {
		t.pool.ReturnSession(s, err)
	}()
	return s.QueueBind(ctx, name, routingKey, exchange, option...)
}

// QueueUnbind removes a binding between an exchange and queue matching the key and
// arguments.

// It is possible to send and empty string for the exchange name which means to
// unbind the queue from the default exchange.
func (t *Topologer) QueueUnbind(ctx context.Context, name string, routingKey string, exchange string, args ...types.Table) (err error) {
	s, err := t.getSession(ctx)
	if err != nil {
		return err
	}
	defer func() {
		t.pool.ReturnSession(s, err)
	}()
	return s.QueueUnbind(ctx, name, routingKey, exchange, args...)
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
func (t *Topologer) ExchangeBind(ctx context.Context, destination string, routingKey string, source string, option ...types.ExchangeBindOptions) (err error) {
	s, err := t.getSession(ctx)
	if err != nil {
		return err
	}
	defer func() {
		t.pool.ReturnSession(s, err)
	}()
	return s.ExchangeBind(ctx, destination, routingKey, source, option...)
}

// ExchangeUnbind unbinds the destination exchange from the source exchange on the
// server by removing the routing key between them.  This is the inverse of
// ExchangeBind.  If the binding does not currently exist, an error will be
// returned.
func (t *Topologer) ExchangeUnbind(ctx context.Context, destination string, routingKey string, source string, option ...types.ExchangeUnbindOptions) (err error) {
	s, err := t.getSession(ctx)
	if err != nil {
		return err
	}
	defer func() {
		t.pool.ReturnSession(s, err)
	}()

	return s.ExchangeUnbind(ctx, destination, routingKey, source, option...)
}
