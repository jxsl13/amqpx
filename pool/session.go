package pool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jxsl13/amqpx/logging"
	"github.com/rabbitmq/amqp091-go"
)

const (
	notImplemented = 540
)

// Session is
type Session struct {
	name        string
	cached      bool
	confirmable bool
	bufferSize  int

	channel  *amqp091.Channel
	confirms chan amqp091.Confirmation
	errors   chan *amqp091.Error

	conn          *Connection
	autoCloseConn bool

	consumers map[string]bool // saves consumer names in order to cancel them upon session closure

	mu     sync.Mutex
	ctx    context.Context
	cancel context.CancelFunc

	log logging.Logger
}

// NewSession wraps a connection and a channel in order tointeract with the message broker.
// By default the context of the parent connection is used for cancellation.
func NewSession(conn *Connection, name string, options ...SessionOption) (*Session, error) {
	if conn.IsClosed() {
		return nil, ErrClosed
	}

	// default values
	option := sessionOption{
		Logger:      conn.log, // derive logger form connection
		Cached:      false,
		Confirmable: false,
		BufferSize:  100,
		// derive context from connection, as we are derived from the connection
		// so in case the connection is closed, we are closed as well.
		Ctx:           conn.ctx,
		AutoCloseConn: false, // do not close the connection provided by caller, by default
	}

	// override default values if options were provided
	for _, o := range options {
		o(&option)
	}

	ctx, cancel := context.WithCancel(option.Ctx)

	session := &Session{
		name:        name,
		cached:      option.Cached,
		confirmable: option.Confirmable,
		bufferSize:  option.BufferSize,

		consumers: map[string]bool{},
		channel:   nil, // will be created below
		confirms:  nil, // will be created below
		errors:    nil, // will be created below

		conn:          conn,
		autoCloseConn: option.AutoCloseConn,

		ctx:    ctx,
		cancel: cancel,

		log: option.Logger,
	}

	err := session.Connect()
	if err != nil {
		return nil, fmt.Errorf("failed to create session: %w", err)
	}
	return session, nil
}

// Close closes the session completely.
// Do not use this method in case you have acquired the session
// from a connection pool.
// Use the ConnectionPool.ResurnSession method in order to return the session.
func (s *Session) Close() (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.debug("closing session...")
	defer func() {
		if err != nil {
			s.warn(err, "closed")
		} else {
			s.info("closed")
		}
	}()

	s.cancel()
	if s.channel == nil || s.channel.IsClosed() {
		return nil
	}

	for consumer := range s.consumers {
		// ignore error, as at this point we cannot do anything about the error
		// tell server to cancel consumer deliveries.
		_ = s.channel.Cancel(consumer, false)
	}

	if s.autoCloseConn {
		_ = s.channel.Close()
		return s.conn.Close()
	}

	return s.channel.Close()
}

func (s *Session) Name() string {
	return s.name
}

// Connect tries to create (or re-create) the channel from the Connection it is derived from.
func (s *Session) Connect() (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.connect()
}

func (s *Session) connect() (err error) {
	defer func() {
		// reset state in case of an error
		if err != nil {
			s.channel = nil
			s.errors = make(chan *amqp091.Error)
			s.confirms = make(chan amqp091.Confirmation)

			close(s.errors)
			close(s.confirms)

			s.warn(err, "failed to open session")
		} else {
			s.info("opened session")
		}
	}()
	s.debug("opening session...")

	if s.conn.IsClosed() {
		// do not reconnect connection explicitly
		return ErrClosed
	}

	channel, err := s.conn.channel()
	if err != nil {
		return fmt.Errorf("%w: %v", ErrConnectionFailed, err)
	}

	if s.confirmable {
		s.confirms = make(chan amqp091.Confirmation, s.bufferSize)
		channel.NotifyPublish(s.confirms)

		err = channel.Confirm(false)
		if err != nil {
			return err
		}
	}

	s.errors = make(chan *amqp091.Error, s.bufferSize)
	channel.NotifyClose(s.errors)

	// reset consumer tracking upon reconnect
	s.consumers = map[string]bool{}
	s.channel = channel

	return nil

}

func (s *Session) Recover() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.recover()
}

func (s *Session) tryRecover(err error) error {
	if err == nil {
		return nil
	}
	if !recoverable(err) {
		return err
	}
	return s.recover()
}

func (s *Session) recover() error {

	// tries to recover session forever
	for {

		err := s.conn.Recover() // recovers connection with a backoff mechanism
		if err != nil {
			// upon shutdown this will fail
			return fmt.Errorf("failed to recover session: %w", err)
		}

		// no backoff upon retry, because Recover already retries
		// with a backoff. Sessions should be instantly created on a healthy connection
		err = s.connect() // Creates a new channel and flushes internal buffers automatically.
		if err != nil {
			continue
		}
		break
	}

	return nil
}

// AwaitConfirm tries to await a confirmation from the broker for a published message
// You may check for ErrNack in order to see whether the broker rejected the message temporatily.
// AwaitConfirm cannot be retried in case the channel dies.
// You must resend your message and attempt to await it again.
func (s *Session) AwaitConfirm(ctx context.Context, expectedTag uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.confirmable {
		return ErrNoConfirms
	}

	select {
	case confirm, ok := <-s.confirms:
		if !ok {
			return fmt.Errorf("confirms channel %w", ErrClosed)
		}
		if !confirm.Ack {

			// in case the server did not accept the message, it might be due to
			// resource problems.
			// TODO: do we want to pause here upon flow control messages
			s.conn.PauseOnFlowControl()
			return ErrNack
		}
		if confirm.DeliveryTag != expectedTag {
			return fmt.Errorf("%w: expected %d, got %d", ErrDeliveryTagMismatch, expectedTag, confirm.DeliveryTag)
		}
		return nil
	case <-ctx.Done():
		err := ctx.Err()
		return fmt.Errorf("await context %w: %v", ErrClosed, err)
	case <-s.ctx.Done():
		err := ctx.Err()
		return fmt.Errorf("session %w: %v", ErrClosed, err)
	}
}

// Publishing captures the client message sent to the server.  The fields
// outside of the Headers table included in this struct mirror the underlying
// fields in the content frame.  They use native types for convenience and
// efficiency.
type Publishing struct {
	// Application or exchange specific fields,
	// the headers exchange will inspect this field.
	Headers amqp091.Table

	// Properties
	ContentType     string    // MIME content type
	ContentEncoding string    // MIME content encoding
	DeliveryMode    uint8     // Transient (0 or 1) or Persistent (2)
	Priority        uint8     // 0 to 9
	CorrelationId   string    // correlation identifier
	ReplyTo         string    // address to to reply to (ex: RPC)
	Expiration      string    // message expiration spec
	MessageId       string    // message identifier
	Timestamp       time.Time // message timestamp
	Type            string    // message type name
	UserId          string    // creating user id - ex: "guest"
	AppId           string    // creating application id

	// The application specific payload of the message
	Body []byte

	// Since publishings are asynchronous, any undeliverable message will get returned by the server.
	// Add a listener with Channel.NotifyReturn to handle any undeliverable message when calling publish with either the mandatory or immediate parameters as true.
	// Publishings can be undeliverable when the mandatory flag is true and no queue is bound that matches the routing key,
	// or when the immediate flag is true and no consumer on the matched queue is ready to accept the delivery.
	// This can return an error when the channel, connection or socket is closed. The error or lack of an error does not indicate whether the server has received this publishing.
	Mandatory bool
	Immediate bool
}

// Publish sends a Publishing from the client to an exchange on the server.
// When you want a single message to be delivered to a single queue, you can publish to the default exchange with the routingKey of the queue name.
// This is because every declared queue gets an implicit route to the default exchange.
// It is possible for publishing to not reach the broker if the underlying socket is shut down without pending publishing packets being flushed from the kernel buffers.
// The easy way of making it probable that all publishings reach the server is to always call Connection.Close before terminating your publishing application.
// The way to ensure that all publishings reach the server is to add a listener to Channel.NotifyPublish and put the channel in confirm mode with Channel.Confirm.
// Publishing delivery tags and their corresponding confirmations start at 1. Exit when all publishings are confirmed.
// When Publish does not return an error and the channel is in confirm mode, the internal counter for DeliveryTags with the first confirmation starts at 1.
func (s *Session) Publish(ctx context.Context, exchange string, routingKey string, msg Publishing) (tag uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.retryPublish(func() (uint64, error) {
		tag = 0
		if s.confirmable {
			tag = s.channel.GetNextPublishSeqNo()
		}

		err = s.channel.PublishWithContext(
			ctx,
			exchange,
			routingKey,
			msg.Mandatory,
			msg.Immediate,
			amqp091.Publishing{
				Headers:         msg.Headers,
				ContentType:     msg.ContentType,
				ContentEncoding: msg.ContentEncoding,
				DeliveryMode:    msg.DeliveryMode,
				Priority:        msg.Priority,
				CorrelationId:   msg.CorrelationId,
				ReplyTo:         msg.ReplyTo,
				Expiration:      msg.Expiration,
				MessageId:       msg.MessageId,
				Timestamp:       msg.Timestamp,
				Type:            msg.Type,
				UserId:          msg.UserId,
				AppId:           msg.AppId,
				Body:            msg.Body,
			},
		)
		if err != nil {
			return 0, err
		}
		return tag, err
	})
}

func (s *Session) retryPublish(f func() (uint64, error)) (tag uint64, err error) {
	for {
		tag, err := f()
		if err == nil {
			return tag, nil
		}
		err = s.tryRecover(err)
		if err != nil {
			return 0, err
		}
	}
}

// Get is only supposed to be used for testing purposes, do not us eit to poll the queue periodically.
func (s *Session) Get(queue string, autoAck bool) (msg *amqp091.Delivery, ok bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.retryGet(func() (*amqp091.Delivery, bool, error) {
		m, ok, err := s.channel.Get(queue, autoAck)
		return &m, ok, err
	})
}

func (s *Session) retryGet(f func() (*amqp091.Delivery, bool, error)) (msg *amqp091.Delivery, ok bool, err error) {
	for {
		msg, ok, err := f()
		if err == nil {
			return msg, ok, nil
		}
		err = s.tryRecover(err)
		if err != nil {
			return nil, false, err
		}
	}
}

// Nack rejects the message.
// In case the underlying channel dies, you cannot send a nack for the processed message.
// You might receive the message again from the broker, as it expects a n/ack
func (s *Session) Nack(deliveryTag uint64, multiple bool, requeue bool) (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.channel.Nack(deliveryTag, multiple, requeue)
}

// Ack confirms the processing of the message.
// In case the underlying channel dies, you cannot send a nack for the processed message.
// You might receive the message again from the broker, as it expects a n/ack
func (s *Session) Ack(deliveryTag uint64, multiple bool) (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.channel.Ack(deliveryTag, multiple)
}

type ConsumeOptions struct {
	// The consumer is identified by a string that is unique and scoped for all consumers on this channel. If you wish to eventually cancel the consumer, use the same non-empty identifier in Channel.Cancel.
	// An empty string will cause the library to generate a unique identity.
	// The consumer identity will be included in every Delivery in the ConsumerTag field
	ConsumerTag string
	// When AutoAck (also known as noAck) is true, the server will acknowledge deliveries to this consumer prior to writing the delivery to the network. When autoAck is true, the consumer should not call Delivery.Ack.
	// Automatically acknowledging deliveries means that some deliveries may get lost if the consumer is unable to process them after the server delivers them. See http://www.rabbitmq.com/confirms.html for more details.
	AutoAck bool
	// When Exclusive is true, the server will ensure that this is the sole consumer from this queue. When exclusive is false, the server will fairly distribute deliveries across multiple consumers.
	Exclusive bool
	// The NoLocal flag is not supported by RabbitMQ.
	// It's advisable to use separate connections for Channel.Publish and Channel.Consume so not to have TCP pushback on publishing affect the ability to consume messages, so this parameter is here mostly for completeness.
	NoLocal bool
	// When NoWait is true, do not wait for the server to confirm the request and immediately begin deliveries. If it is not possible to consume, a channel exception will be raised and the channel will be closed.
	// Optional arguments can be provided that have specific semantics for the queue or server.
	NoWait bool
	// Args are aditional implementation dependent parameters.
	Args amqp091.Table
}

// Consume immediately starts delivering queued messages.
//
// Begin receiving on the returned chan Delivery before any other operation on the Connection or Channel.
// Continues deliveries to the returned chan Delivery until Channel.Cancel, Connection.Close, Channel.Close, or an AMQP exception occurs.
// Consumers must range over the chan to ensure all deliveries are received.
//
// Unreceived deliveries will block all methods on the same connection.
// All deliveries in AMQP must be acknowledged.
// It is expected of the consumer to call Delivery.Ack after it has successfully processed the delivery.
//
// If the consumer is cancelled or the channel or connection is closed any unacknowledged deliveries will be requeued at the end of the same queue.
//
// Inflight messages, limited by Channel.Qos will be buffered until received from the returned chan.
// When the Channel or Connection is closed, all buffered and inflight messages will be dropped.
// When the consumer identifier tag is cancelled, all inflight messages will be delivered until the returned chan is closed.
func (s *Session) Consume(queue string, option ...ConsumeOptions) (<-chan amqp091.Delivery, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// defaults
	o := ConsumeOptions{
		AutoAck:   false,
		Exclusive: false,
		NoLocal:   false, // not used by RabbitMQ
		NoWait:    false,
		Args:      nil,
	}
	if len(option) > 0 {
		o = option[0]
	}

	if o.ConsumerTag == "" {
		// use our own consumer naming
		o.ConsumerTag = s.Name()

	}

	// retries to connect and attempts to start a consumer
	c, err := s.consumeRetry(func() (<-chan amqp091.Delivery, error) {
		return s.channel.Consume(
			queue,
			o.ConsumerTag,
			o.AutoAck,
			o.Exclusive,
			o.NoLocal,
			o.NoWait,
			o.Args,
		)
	})
	if err != nil {
		return nil, err
	}
	s.consumers[o.ConsumerTag] = true

	return c, nil
}

func (s *Session) consumeRetry(f func() (<-chan amqp091.Delivery, error)) (<-chan amqp091.Delivery, error) {
	for {
		c, err := f()
		if err == nil {
			return c, nil
		}
		err = s.tryRecover(err)
		if err != nil {
			return nil, err
		}
	}
}

func (s *Session) retry(f func() error) error {
	for {
		err := f()
		if err == nil {
			return nil
		}
		err = s.tryRecover(err)
		if err != nil {
			return err
		}
	}
}

func recoverable(err error) bool {

	if ae, ok := err.(*amqp091.Error); ok {
		switch ae.Code {
		case notImplemented:
			return false
		default:
			// not recoverable by changing user input
			// because of potential connection loss
			return !ae.Recover
		}
	}

	return errors.Is(err, amqp091.ErrClosed)
}

type ExchangeDeclareOptions struct {
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
	Durable bool
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
	AutoDelete bool
	// Exchanges declared as `internal` do not accept accept publishings. Internal
	// exchanges are useful when you wish to implement inter-exchange topologies
	// that should not be exposed to users of the broker.
	Internal bool
	// When NoWait is true, declare without waiting for a confirmation from the server.
	// The channel may be closed as a result of an error.  Add a NotifyClose listener
	// to respond to any exceptions.
	NoWait bool
	// Optional amqp091.Table of arguments that are specific to the server's implementation of
	// the exchange can be sent for exchange types that require extra parameters.
	Args amqp091.Table
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
func (s *Session) ExchangeDeclare(name string, kind string, option ...ExchangeDeclareOptions) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// sane defaults
	o := ExchangeDeclareOptions{
		Durable:    true,
		AutoDelete: false,
		Internal:   false,
		NoWait:     false,
		Args:       nil,
	}
	if len(option) > 0 {
		o = option[0]
	}

	return s.retry(func() error {
		return s.channel.ExchangeDeclare(
			name,
			kind,
			o.Durable,
			o.AutoDelete,
			o.Internal,
			o.NoWait,
			o.Args,
		)
	})
}

type ExchangeDeleteOptions struct {
	// When IfUnused is true, the server will only delete the exchange if it has no queue
	// bindings.  If the exchange has queue bindings the server does not delete it
	// but close the channel with an exception instead.  Set this to true if you are
	// not the sole owner of the exchange.
	IfUnused bool
	// When NoWait is true, do not wait for a server confirmation that the exchange has
	// been deleted.
	NoWait bool
}

// ExchangeDelete removes the named exchange from the server. When an exchange is
// deleted all queue bindings on the exchange are also deleted.  If this exchange
// does not exist, the channel will be closed with an error.
func (s *Session) ExchangeDelete(name string, option ...ExchangeDeleteOptions) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	o := ExchangeDeleteOptions{
		IfUnused: false,
		NoWait:   false,
	}
	if len(option) > 0 {
		o = option[0]
	}

	return s.retry(func() error {
		return s.channel.ExchangeDelete(name, o.IfUnused, o.NoWait)
	})
}

// QueueDeclareOptions can be passed to the queue declaration
// If you want to change your default queue behavior.
type QueueDeclareOptions struct {
	// Durable and Non-Auto-Deleted queues will survive server restarts and remain
	// when there are no remaining consumers or bindings.  Persistent publishings will
	// be restored in this queue on server restart.  These queues are only able to be
	// bound to durable exchanges.
	//
	// Non-Durable and Non-Auto-Deleted queues will remain declared as long as the
	// server is running regardless of how many consumers.  This lifetime is useful
	// for temporary topologies that may have long delays between consumer activity.
	// These queues can only be bound to non-durable exchanges.
	//
	// Durable and Auto-Deleted queues will be restored on server restart, but without
	// active consumers will not survive and be removed.  This Lifetime is unlikely
	// to be useful.
	Durable bool
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
	AutoDelete bool
	// Exclusive queues are only accessible by the connection that declares them and
	// will be deleted when the connection closes.  Channels on other connections
	// will receive an error when attempting  to declare, bind, consume, purge or
	// delete a queue with the same name.
	Exclusive bool
	// When noWait is true, the queue will assume to be declared on the server.  A
	// channel exception will arrive if the conditions are met for existing queues
	// or attempting to modify an existing queue from a different connection.
	//
	NoWait bool
	// Args are additional properties you can set, like the queue type.
	Args amqp091.Table
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
// When the error return value is not nil, you can assume the queue could not be
// declared with these parameters, and the channel will be closed.
func (s *Session) QueueDeclare(name string, option ...QueueDeclareOptions) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	o := QueueDeclareOptions{
		Durable:    true,
		AutoDelete: false,
		Exclusive:  false,
		NoWait:     false,
		Args:       QuorumQueue,
	}
	if len(option) > 0 {
		o = option[0]
	}

	return s.retry(func() error {
		_, err := s.channel.QueueDeclare(
			name,
			o.Durable,
			o.AutoDelete,
			o.Exclusive,
			o.NoWait,
			o.Args,
		)
		return err
	})
}

// QueueDeleteOptions are options for deleting a queue.
type QueueDeleteOptions struct {
	// When IfUnused is true, the queue will not be deleted if there are any
	// consumers on the queue.  If there are consumers, an error will be returned and
	// the channel will be closed.
	IfUnused bool
	// When IfEmpty is true, the queue will not be deleted if there are any messages
	// remaining on the queue.  If there are messages, an error will be returned and
	// the channel will be closed.
	IfEmpty bool
	// When NoWait is true, the queue will be deleted without waiting for a response
	// from the server.  The purged message count will not be meaningful. If the queue
	// could not be deleted, a channel exception will be raised and the channel will
	// be closed.
	NoWait bool
}

// QueueDelete removes the queue from the server including all bindings then
// purges the messages based on server configuration, returning the number of
// messages purged.
func (s *Session) QueueDelete(name string, option ...QueueDeleteOptions) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	o := QueueDeleteOptions{
		IfUnused: false,
		IfEmpty:  false,
		NoWait:   false,
	}
	if len(option) > 0 {
		o = option[0]
	}

	return s.retryQueueDelete(func() (int, error) {
		return s.channel.QueueDelete(
			name,
			o.IfUnused,
			o.IfEmpty,
			o.NoWait,
		)
	})
}

func (s *Session) retryQueueDelete(f func() (int, error)) (int, error) {
	for {
		i, err := f()
		if err == nil {
			return i, nil
		}
		err = s.tryRecover(err)
		if err != nil {
			return 0, err
		}
	}
}

type QueueBindOptions struct {
	// When NoWait is false and the queue could not be bound, the channel will be
	// closed with an error.
	NoWait bool
	// Additional implementation specific arguments
	Args amqp091.Table
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
func (s *Session) QueueBind(queueName string, routingKey string, exchange string, option ...QueueBindOptions) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// sane defaults
	o := QueueBindOptions{
		NoWait: false,
		Args:   nil,
	}

	if len(option) > 0 {
		o = option[0]
	}

	return s.retry(func() error {
		return s.channel.QueueBind(
			queueName,
			routingKey,
			exchange,
			o.NoWait,
			o.Args,
		)
	})
}

// QueueUnbind removes a binding between an exchange and queue matching the key and
// arguments.
// It is possible to send and empty string for the exchange name which means to
// unbind the queue from the default exchange.
func (s *Session) QueueUnbind(name string, routingKey string, exchange string, arg ...amqp091.Table) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// default
	var option amqp091.Table = nil
	if len(arg) > 0 {
		option = arg[0]
	}

	return s.retry(func() error {
		return s.channel.QueueUnbind(name, routingKey, exchange, option)
	})
}

type ExchangeBindOptions struct {
	// When NoWait is true, do not wait for the server to confirm the binding.  If any
	// error occurs the channel will be closed.  Add a listener to NotifyClose to
	// handle these errors.
	NoWait bool

	// Optional arguments specific to the exchanges bound can also be specified.
	Args amqp091.Table
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
func (s *Session) ExchangeBind(destination string, routingKey string, source string, option ...ExchangeBindOptions) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// defaults
	o := ExchangeBindOptions{
		NoWait: false,
		Args:   nil,
	}
	if len(option) > 0 {
		o = option[0]
	}

	return s.retry(func() error {
		return s.channel.ExchangeBind(
			destination,
			routingKey,
			source,
			o.NoWait,
			o.Args,
		)
	})
}

// ExchangeUnbindOptions can be used to configure additional unbind options.
type ExchangeUnbindOptions struct {
	// When NoWait is true, do not wait for the server to confirm the deletion of the
	// binding.  If any error occurs the channel will be closed.  Add a listener to
	// NotifyClose to handle these errors.
	NoWait bool

	// Optional arguments that are specific to the type of exchanges bound can also be
	// provided.  These must match the same arguments specified in ExchangeBind to
	// identify the binding.
	Args amqp091.Table
}

// ExchangeUnbind unbinds the destination exchange from the source exchange on the
// server by removing the routing key between them.  This is the inverse of
// ExchangeBind.  If the binding does not currently exist, an error will be
// returned.
func (s *Session) ExchangeUnbind(destination string, routingKey string, source string, option ...ExchangeUnbindOptions) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	o := ExchangeUnbindOptions{
		NoWait: false,
		Args:   nil,
	}
	if len(option) > 0 {
		o = option[0]
	}

	return s.retry(func() error {
		return s.channel.ExchangeUnbind(
			destination,
			routingKey,
			source,
			o.NoWait,
			o.Args,
		)
	})
}

// Flow allows to enable or disable flow from the message broker
// Flow pauses the delivery of messages to consumers on this channel.  Channels
// are opened with flow control active, to open a channel with paused
// deliveries immediately call this method with `false` after calling
// Connection.Channel.
//
// When active is `false`, this method asks the server to temporarily pause deliveries
// until called again with active as `true`.
//
// Channel.Get methods will not be affected by flow control.
//
// This method is not intended to act as window control.  Use Channel.Qos to limit
// the number of unacknowledged messages or bytes in flight instead.
//
// The server may also send us flow methods to throttle our publishings.  A well
// behaving publishing client should add a listener with Channel.NotifyFlow and
// pause its publishings when `false` is sent on that channel.
//
// Note: RabbitMQ prefers to use TCP push back to control flow for all channels on
// a connection, so under high volume scenarios, it's wise to open separate
// Connections for publishings and deliveries.
func (s *Session) Flow(active bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.retry(func() error {
		return s.channel.Flow(active)
	})
}

// flushConfirms removes all previous confirmations pending processing.
// You can use the returned value
func (s *Session) flushConfirms() []amqp091.Confirmation {
	s.mu.Lock()
	defer s.mu.Unlock()

	confirms := make([]amqp091.Confirmation, 0, len(s.confirms))
flush:
	for {
		// Some weird use case where the Channel is being flooded with confirms after connection disruption
		// It lead to an infinite loop when this method was called.
		select {
		case c, ok := <-s.confirms:
			if !ok {
				break flush
			}
			// flush confirmations in channel
			confirms = append(confirms, c)
		case <-s.catchShutdown():
			break flush
		default:
			break flush
		}
	}
	return confirms
}

// Error returns the first error from the errors channel
// and flushes all other pending errors from the channel
// In case that there are no errors, nil is returned.
func (s *Session) Error() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.error()
}

// not threadsafe
func (s *Session) error() error {
	var (
		err error = nil
	)
	for {
		select {
		case <-s.catchShutdown():
			return fmt.Errorf("session %w", ErrClosed)
		case e, ok := <-s.errors:
			if !ok {
				return amqp091.ErrClosed
			}
			// only overwrite with the first error
			if err == nil {
				err = e
			} else {
				// flush all other errors after the first one
				continue
			}
		default:
			return err
		}
	}
}

// IsCached returns true in case this session is supposed to be returned to a session pool.
func (s *Session) IsCached() bool {
	return s.cached
}

// IsConfirmable returns true in case this session requires that after Publishing a message you also MUST Await its confirmation
func (s *Session) IsConfirmable() bool {
	return s.confirmable
}

func (s *Session) catchShutdown() <-chan struct{} {
	// no locking because
	return s.ctx.Done()
}

func (s *Session) info(a ...any) {
	s.log.WithField("connection", s.conn.Name()).WithField("session", s.Name()).Info(a...)
}

func (s *Session) warn(err error, a ...any) {
	s.log.WithField("connection", s.conn.Name()).WithField("session", s.Name()).WithField("error", err.Error()).Warn(a...)
}

func (s *Session) debug(a ...any) {
	s.log.WithField("connection", s.conn.Name()).WithField("session", s.Name()).Debug(a...)
}
