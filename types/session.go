package types

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jxsl13/amqpx/internal/contextutils"
	"github.com/jxsl13/amqpx/internal/errorutils"
	"github.com/rabbitmq/amqp091-go"
)

// Session is a wrapper for an amqp channel.
// It MUST not be used in a multithreaded context, but only in a single goroutine.
type Session struct {
	name           string
	cached         bool
	flagged        bool
	confirmable    bool
	bufferCapacity int

	channel  *amqp091.Channel
	returned chan amqp091.Return
	confirms chan amqp091.Confirmation
	errors   chan *amqp091.Error

	conn          *Connection
	autoCloseConn bool

	consumers map[string]bool // saves consumer names in order to cancel them upon session closure

	// a session should not be used in a multithreaded context
	// but only one session per goroutine. That is why we keep this
	// as a Mutex and not a RWMutex.
	mu     sync.Mutex
	ctx    context.Context
	cancel context.CancelFunc

	log *slog.Logger

	recoverCB                     sessionRetryCallback
	publishRetryCB                sessionRetryCallback
	getRetryCB                    sessionRetryCallback
	consumeContextRetryCB         sessionRetryCallback
	exchangeDeclareRetryCB        sessionRetryCallback
	exchangeDeclarePassiveRetryCB sessionRetryCallback
	exchangeDeleteRetryCB         sessionRetryCallback
	queueDeclareRetryCB           sessionRetryCallback
	queueDeclarePassiveRetryCB    sessionRetryCallback
	queueDeleteRetryCB            sessionRetryCallback
	queueBindRetryCB              sessionRetryCallback
	queueUnbindRetryCB            sessionRetryCallback
	queuePurgeRetryCB             sessionRetryCallback
	exchangeBindRetryCB           sessionRetryCallback
	exchangeUnbindRetryCB         sessionRetryCallback
	qosRetryCB                    sessionRetryCallback
	flowRetryCB                   sessionRetryCallback
}

type sessionRetryCallback func(connName, sessionName string, retry int, err error)

// we want to define the operation names at a somewhat central location.
func newSessionRetryCallback(operation string, cb SessionRetryCallback) sessionRetryCallback {
	if cb == nil {
		return nil
	}
	return func(connName, sessionName string, retry int, err error) {
		cb(operation, connName, sessionName, retry, err)
	}
}

// NewSession wraps a connection and a channel in order tointeract with the message broker.
// By default the context of the parent connection is used for cancellation.
func NewSession(conn *Connection, name string, options ...SessionOption) (*Session, error) {
	if conn.IsClosed() {
		return nil, ErrClosed
	}

	// default values
	option := sessionOption{
		Logger:         conn.log, // derive logger form connection
		Cached:         false,
		Confirmable:    false,
		BufferCapacity: 10,
		// derive context from connection, as we are derived from the connection
		// so in case the connection is closed, we are closed as well.
		Ctx:           conn.ctx,
		AutoCloseConn: false, // do not close the connection provided by caller, by default
	}

	// override default values if options were provided
	for _, o := range options {
		o(&option)
	}

	ctx, cc := context.WithCancelCause(option.Ctx)
	cancel := contextutils.ToCancelFunc(fmt.Errorf("session %w", ErrClosed), cc)

	session := &Session{
		name:           name,
		cached:         option.Cached,
		confirmable:    option.Confirmable,
		bufferCapacity: option.BufferCapacity,

		consumers: map[string]bool{},
		channel:   nil, // will be created on connect
		errors:    nil, // will be created on connect
		confirms:  nil, // will be created on connect
		returned:  nil, // will be created on connect

		conn:          conn,
		autoCloseConn: option.AutoCloseConn,

		ctx:    ctx,
		cancel: cancel,

		log: option.Logger,

		recoverCB:                     newSessionRetryCallback("recover", option.RecoverCallback),
		publishRetryCB:                newSessionRetryCallback("publish", option.PublishRetryCallback),
		getRetryCB:                    newSessionRetryCallback("get", option.GetRetryCallback),
		consumeContextRetryCB:         newSessionRetryCallback("consume", option.ConsumeContextRetryCallback),
		exchangeDeclareRetryCB:        newSessionRetryCallback("exchange_declare", option.ExchangeDeclareRetryCallback),
		exchangeDeclarePassiveRetryCB: newSessionRetryCallback("exchange_declare_passive", option.ExchangeDeclarePassiveRetryCallback),
		exchangeDeleteRetryCB:         newSessionRetryCallback("exchange_delete", option.ExchangeDeleteRetryCallback),
		queueDeclareRetryCB:           newSessionRetryCallback("queue_declare", option.QueueDeclareRetryCallback),
		queueDeclarePassiveRetryCB:    newSessionRetryCallback("queue_declare_passive", option.QueueDeclarePassiveRetryCallback),
		queueDeleteRetryCB:            newSessionRetryCallback("queue_delete", option.QueueDeleteRetryCallback),
		queueBindRetryCB:              newSessionRetryCallback("queue_bind", option.QueueBindRetryCallback),
		queueUnbindRetryCB:            newSessionRetryCallback("queue_unbind", option.QueueUnbindRetryCallback),
		queuePurgeRetryCB:             newSessionRetryCallback("queue_purge", option.QueuePurgeRetryCallback),
		exchangeBindRetryCB:           newSessionRetryCallback("exchange_bind", option.ExchangeBindRetryCallback),
		exchangeUnbindRetryCB:         newSessionRetryCallback("exchange_unbind", option.ExchangeUnbindRetryCallback),
		qosRetryCB:                    newSessionRetryCallback("qos", option.QoSRetryCallback),
		flowRetryCB:                   newSessionRetryCallback("flow", option.FlowRetryCallback),
	}

	err := session.Connect()
	if err != nil {
		return nil, fmt.Errorf("failed to create session: %w", err)
	}
	return session, nil
}

// Flag marks the session as flagged.
// This is useful in case of a connection pool, where the session is returned to the pool
// and should be recovered by the next user.
func (s *Session) Flag(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	flagged := err != nil && errorutils.Recoverable(err)
	if !s.flagged && flagged {
		s.flagged = flagged
	}
}

// IsFlagged returns whether the session is flagged.
func (s *Session) IsFlagged() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.flagged
}

// Close closes the session completely.
// Do not use this method in case you have acquired the session
// from a connection pool.
// Use the ConnectionPool.ResurnSession method in order to return the session.
func (s *Session) Close() (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	log := s.slog()

	log.Debug("closing session...")
	defer func() {
		if err != nil {
			log.Warn(fmt.Sprintf("failed to close session: %s", err.Error()))
		} else {
			log.Info("closed")
		}
	}()

	if s.autoCloseConn {
		defer func() {
			log.Debug("closing session connection...")
			err = errors.Join(err, s.conn.Close())
		}()
	}
	log.Debug("closing session context...")
	s.cancel()
	return s.close()
}

func (s *Session) close() (err error) {
	log := s.slog()
	defer func() {
		log.Debug("flushing channels...")
		flush(s.errors)
		flush(s.confirms)
		flush(s.returned)

		if s.channel != nil {
			s.channel = nil
		}
	}()

	if s.channel == nil || s.channel.IsClosed() {
		return nil
	}

	if len(s.consumers) > 0 {
		log.Debug("canceling consumers...")
		for consumer := range s.consumers {
			// ignore error, as at this point we cannot do anything about the error
			// tell server to cancel consumer deliveries.
			cerr := s.channel.Cancel(consumer, false)
			err = errors.Join(err, cerr)
		}
	}

	log.Debug("closing amqp channel...")
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
	log := s.slog()
	log.Debug("opening session...")
	defer func() {
		// reset state in case of an error
		if err != nil {
			err = errors.Join(err, s.close())
			log.Error(fmt.Sprintf("failed to open session: %s", err.Error()))
		} else {
			log.Info("opened session")
		}
	}()

	if s.conn.IsClosed() {
		// do not reconnect connection explicitly
		return ErrClosed
	}

	cerr := s.close() // close any open rabbitmq channel & cleanup Go channels
	if cerr != nil {
		log.Error(fmt.Sprintf("failed to close session before recreating it: %s", cerr.Error()))
	}

	channel, err := s.conn.channel()
	if err != nil {
		return fmt.Errorf("%v: %w", ErrConnectionFailed, err)
	}

	s.errors = make(chan *amqp091.Error, s.bufferCapacity)
	channel.NotifyClose(s.errors)

	if s.confirmable {
		s.confirms = make(chan amqp091.Confirmation, s.bufferCapacity)
		channel.NotifyPublish(s.confirms)
		err = channel.Confirm(false)
		if err != nil {
			return err
		}

		s.returned = make(chan amqp091.Return, s.bufferCapacity)
		channel.NotifyReturn(s.returned)
	}

	// reset consumer tracking upon reconnect
	s.consumers = map[string]bool{}
	s.channel = channel

	return nil

}

func (s *Session) Recover(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.recover(ctx)
}

func (s *Session) tryRecover(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}

	// shutdown & context cancelation is not handled in this function
	if !errorutils.Recoverable(err) {
		return err
	}

	// shutdown & context cancelation is handled in this function
	return s.recover(ctx)
}

func (s *Session) recover(ctx context.Context) error {

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.catchShutdown():
		return fmt.Errorf("failed to recover session: %w", s.shutdownErr())
	default:
		// check if context was closed before starting a recovery.
	}

	// check if session/channel needs to be recovered
	err := s.error()
	if err == nil {
		return nil
	}
	log := s.slog()
	log.Warn(fmt.Sprintf("recovering session due to error: %s", err.Error()))

	// necessary for cleanup and to cleanup potentially dangling open sessions
	// already ran into a bug, where recovery spawned infinitely many channels.
	cerr := s.close()
	if cerr != nil {
		log.Error(fmt.Sprintf("failed to close session before recovering it: %s", cerr.Error()))
	}

	// tries to recover session forever
	for try := 0; ; try++ {

		if s.recoverCB != nil {
			// allow a user to hook into the recovery process of a session
			// this is expected to not be called often, as the connection should be the main cause
			// of errors and not necessarily the session.
			s.recoverCB(s.conn.Name(), s.name, try, err)
		}

		err = s.conn.Recover(ctx) // recovers connection with a backoff mechanism
		if err != nil {
			// upon shutdown this will fail
			return fmt.Errorf("failed to recover session: %w", err)
		}

		// no backoff upon retry, because Recover already retries
		// with a backoff. Sessions should be instantly created on a healthy connection
		err = s.connect() // Creates a new channel and flushes internal buffers automatically.
		if err == nil {
			// successfully recovered
			s.flagged = false
			return nil
		}
	}
}

// AwaitConfirm tries to await a confirmation from the broker for a published message
// You may check for ErrNack in order to see whether the broker rejected the message temporatily.
// WARNING: AwaitConfirm cannot be retried in case the channel dies or errors.
// You must resend your message and attempt to await it again.
func (s *Session) AwaitConfirm(ctx context.Context, expectedTag uint64) (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("await confirm failed: %w", err)
		}
	}()
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.confirmable {
		return ErrNoConfirms
	}

	log := s.slog()

	for {
		select {
		case confirm, ok := <-s.confirms:
			if !ok {
				err := s.error()
				if err != nil {
					return fmt.Errorf("confirms channel closed: %w", err)
				}
				return fmt.Errorf("confirms channel %w", ErrClosed)
			}
			if confirm.DeliveryTag < expectedTag {
				// Stale confirm from a previous publish whose AwaitConfirm was
				// interrupted (e.g., by context cancellation). Discard and keep
				// waiting for our expected tag.
				log.Warn(fmt.Sprintf("discarding stale confirm for tag %d, waiting for tag %d", confirm.DeliveryTag, expectedTag))
				continue
			}
			if confirm.DeliveryTag != expectedTag {
				// https://www.rabbitmq.com/docs/confirms#publisher-confirms-latency
				return fmt.Errorf("%w: expected %d, got %d", ErrDeliveryTagMismatch, expectedTag, confirm.DeliveryTag)
			}
			if !confirm.Ack {
				// in case the server did not accept the message, it might be due to resource problems.
				// TODO: do we want to pause here upon flow control messages?
				return ErrNack
			}
			// confirmed by broker
			return nil
		case returned, ok := <-s.returned:
			if !ok {
				err := s.error()
				if err != nil {
					return fmt.Errorf("returned channel closed: %w", err)
				}
				return fmt.Errorf("%w", errReturnedClosed)
			}
			return fmt.Errorf("%w: %s", ErrReturned, returned.ReplyText)
		case blocking, ok := <-s.conn.BlockingFlowControl():
			if !ok {
				err := s.error()
				if err != nil {
					return fmt.Errorf("blocking channel closed: %w", err)
				}
				return fmt.Errorf("%w", errBlockingFlowControlClosed)
			}
			return fmt.Errorf("%w: %s", ErrBlockingFlowControl, blocking.Reason)
		case <-ctx.Done():
			return fmt.Errorf("await confirm: failed context %w: %w", ErrClosed, ctx.Err())
		case <-s.ctx.Done():
			return fmt.Errorf("session %w: %w", ErrClosed, ctx.Err())
		}
	}
}

// Publishing captures the client message sent to the server.  The fields
// outside of the Headers table included in this struct mirror the underlying
// fields in the content frame.  They use native types for convenience and
// efficiency.
type Publishing struct {
	// Application or exchange specific fields,
	// the headers exchange will inspect this field.
	Headers Table

	// Properties
	ContentType     string    // MIME content type
	ContentEncoding string    // MIME content encoding
	DeliveryMode    uint8     // Persistent (0 or 2) or Transient (1) (different from rabbitmq/amqp091-go library)
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
func (s *Session) Publish(ctx context.Context, exchange string, routingKey string, msg Publishing) (deliveryTag uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// we want to have a persistent messages by default
	// this allows to even in a disaster case where the rabbitmq node is restarted or crashes
	// to still have our messages persisted to disk.
	// https://www.rabbitmq.com/persistence-conf.html#how-it-works
	var amqpDeliverMode uint8
	if msg.DeliveryMode == 1 {
		amqpDeliverMode = 1 // transient (purged upon rabbitmq restart)
	} else {
		amqpDeliverMode = 2 // persistent (persisted to disk upon arrival in queue)
	}

	err = s.retry(ctx, s.publishRetryCB, func() error {
		deliveryTag = 0
		if s.confirmable {
			deliveryTag = s.channel.GetNextPublishSeqNo()
		}

		_, err = s.channel.PublishWithDeferredConfirmWithContext(
			ctx,
			exchange,
			routingKey,
			msg.Mandatory,
			msg.Immediate,
			amqp091.Publishing{
				Headers:         amqp091.Table(msg.Headers),
				ContentType:     msg.ContentType,
				ContentEncoding: msg.ContentEncoding,
				DeliveryMode:    amqpDeliverMode,
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
			return err
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return deliveryTag, nil
}

// PublishBatch sends a list of Publishings from the client to an exchange on the server.
// When you want a batch to be delivered to a single queue, you can publish to the default exchange with the routingKey of the queue name.
// This is because every declared queue gets an implicit route from the default exchange.
// It is possible for publishing to not reach the broker if the underlying socket is shut down without pending publishing packets being flushed from the kernel buffers.
// The easy way of making it probable that all publishings reach the server is to always call Connection.Close before terminating your publishing application.
// The way to ensure that all publishings reach the server is to add a listener to Channel.NotifyPublish and put the channel in confirm mode with Channel.Confirm.
// Publishing delivery tags and their corresponding confirmations start at 1. Exit when all publishings are confirmed.
// When Publish does not return an error and the channel is in confirm mode, the internal counter for DeliveryTags with the first confirmation starts at 1.
func (s *Session) PublishBatch(ctx context.Context, exchange string, routingKey string, msgs []Publishing) (deliveryTags []uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	deliveryTags = make([]uint64, len(msgs))
	err = s.retry(ctx, s.publishRetryCB, func() error {

		for i, msg := range msgs {

			// we want to have a persistent messages by default
			// this allows to even in a disaster case where the rabbitmq node is restarted or crashes
			// to still have our messages persisted to disk.
			// https://www.rabbitmq.com/persistence-conf.html#how-it-works
			var amqpDeliverMode uint8
			if msg.DeliveryMode == 1 {
				amqpDeliverMode = 1 // transient (purged upon rabbitmq restart)
			} else {
				amqpDeliverMode = 2 // persistent (persisted to disk upon arrival in queue)
			}

			if s.confirmable {
				deliveryTags[i] = s.channel.GetNextPublishSeqNo()
			}

			_, err = s.channel.PublishWithDeferredConfirmWithContext(
				ctx,
				exchange,
				routingKey,
				msg.Mandatory,
				msg.Immediate,
				amqp091.Publishing{
					Headers:         amqp091.Table(msg.Headers),
					ContentType:     msg.ContentType,
					ContentEncoding: msg.ContentEncoding,
					DeliveryMode:    amqpDeliverMode,
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
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return deliveryTags, nil
}

// Get is only supposed to be used for testing purposes, do not us eit to poll the queue periodically.
func (s *Session) Get(ctx context.Context, queue string, autoAck bool) (msg Delivery, ok bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var intermediaryMsg amqp091.Delivery
	err = s.retry(ctx, s.getRetryCB, func() error {
		intermediaryMsg, ok, err = s.channel.Get(queue, autoAck)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return Delivery{}, false, err
	}

	return Delivery{
		Headers:         Table(intermediaryMsg.Headers),
		ContentType:     intermediaryMsg.ContentType,
		ContentEncoding: intermediaryMsg.ContentEncoding,
		DeliveryMode:    intermediaryMsg.DeliveryMode,
		Priority:        intermediaryMsg.Priority,
		CorrelationId:   intermediaryMsg.CorrelationId,
		ReplyTo:         intermediaryMsg.ReplyTo,
		Expiration:      intermediaryMsg.Expiration,
		MessageId:       intermediaryMsg.MessageId,
		Timestamp:       intermediaryMsg.Timestamp,
		Type:            intermediaryMsg.Type,
		UserId:          intermediaryMsg.UserId,
		AppId:           intermediaryMsg.AppId,
		ConsumerTag:     intermediaryMsg.ConsumerTag,
		MessageCount:    intermediaryMsg.MessageCount,
		DeliveryTag:     intermediaryMsg.DeliveryTag,
		Redelivered:     intermediaryMsg.Redelivered,
		Exchange:        intermediaryMsg.Exchange,
		RoutingKey:      intermediaryMsg.RoutingKey,
		Body:            intermediaryMsg.Body,
	}, ok, nil
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
	Args Table
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
func (s *Session) ConsumeWithContext(ctx context.Context, queue string, option ...ConsumeOptions) (<-chan amqp091.Delivery, error) {
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

	var (
		c   <-chan amqp091.Delivery
		err error
	)
	// retries to connect and attempts to start a consumer
	err = s.retry(ctx, s.consumeContextRetryCB, func() error {
		c, err = s.channel.ConsumeWithContext(
			ctx,
			queue,
			o.ConsumerTag,
			o.AutoAck,
			o.Exclusive,
			o.NoLocal,
			o.NoWait,
			amqp091.Table(o.Args),
		)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	s.consumers[o.ConsumerTag] = true

	return c, nil
}

func (s *Session) retry(ctx context.Context, cb sessionRetryCallback, f func() error) error {

	for try := 0; ; try++ {
		err := f()
		if err == nil {
			return nil
		}

		if cb != nil {
			cb(s.conn.Name(), s.name, try, err)
		}
		err = s.tryRecover(ctx, err)
		if err != nil {
			return err
		}
	}
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
	// Optional Table of arguments that are specific to the server's implementation of
	// the exchange can be sent for exchange types that require extra parameters.
	Args Table
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
func (s *Session) ExchangeDeclare(ctx context.Context, name string, kind ExchangeKind, option ...ExchangeDeclareOptions) error {
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

	return s.retry(ctx, s.exchangeDeclareRetryCB, func() error {
		return s.channel.ExchangeDeclare(
			name,
			string(kind),
			o.Durable,
			o.AutoDelete,
			o.Internal,
			o.NoWait,
			amqp091.Table(o.Args),
		)
	})
}

// ExchangeDeclarePassive is functionally and parametrically equivalent to
// ExchangeDeclare, except that it sets the "passive" attribute to true. A passive
// exchange is assumed by RabbitMQ to already exist, and attempting to connect to a
// non-existent exchange will cause RabbitMQ to throw an exception. This function
// can be used to detect the existence of an exchange.
func (s *Session) ExchangeDeclarePassive(ctx context.Context, name string, kind ExchangeKind, option ...ExchangeDeclareOptions) error {
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

	err := s.retry(ctx, s.exchangeDeclarePassiveRetryCB, func() error {
		return s.channel.ExchangeDeclarePassive(
			name,
			string(kind),
			o.Durable,
			o.AutoDelete,
			o.Internal,
			o.NoWait,
			amqp091.Table(o.Args),
		)
	})

	if err == nil {
		return nil
	}

	ae := &amqp091.Error{}
	if errors.As(err, &ae) {
		return fmt.Errorf("exchange %w: %w", ErrNotFound, ae)
	}
	return err
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
func (s *Session) ExchangeDelete(ctx context.Context, name string, option ...ExchangeDeleteOptions) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	o := ExchangeDeleteOptions{
		IfUnused: false,
		NoWait:   false,
	}
	if len(option) > 0 {
		o = option[0]
	}

	return s.retry(ctx, s.exchangeDeleteRetryCB, func() error {
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
	Args Table
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
func (s *Session) QueueDeclare(ctx context.Context, name string, option ...QueueDeclareOptions) (Queue, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	o := QueueDeclareOptions{
		Durable:    true,
		AutoDelete: false,
		Exclusive:  false,
		NoWait:     false,
		Args: QuorumQueue.
			WithDeliveryLimit(DefaultQueueDeliveryLimit), // https://www.rabbitmq.com/docs/quorum-queues#repeated-requeues
	}
	if len(option) > 0 {
		o = option[0]
	}
	var (
		err   error
		queue amqp091.Queue
	)
	err = s.retry(ctx, s.queueDeclareRetryCB, func() error {
		queue, err = s.channel.QueueDeclare(
			name,
			o.Durable,
			o.AutoDelete,
			o.Exclusive,
			o.NoWait,
			amqp091.Table(o.Args),
		)
		return err
	})
	if err != nil {
		return Queue{}, err
	}

	return Queue(queue), nil
}

// QueueDeclarePassive is functionally and parametrically equivalent to QueueDeclare, except that it sets the "passive" attribute to true.
// A passive queue is assumed by RabbitMQ to already exist, and attempting to connect to a non-existent queue will cause RabbitMQ to throw an exception.
// This function can be used to test for the existence of a queue.
func (s *Session) QueueDeclarePassive(ctx context.Context, name string, option ...QueueDeclareOptions) (Queue, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	o := QueueDeclareOptions{
		Durable:    true,
		AutoDelete: false,
		Exclusive:  false,
		NoWait:     false,
		Args: QuorumQueue.
			WithDeliveryLimit(DefaultQueueDeliveryLimit), // https://www.rabbitmq.com/docs/quorum-queues#repeated-requeues
	}
	if len(option) > 0 {
		o = option[0]
	}

	var (
		err   error
		queue amqp091.Queue
	)
	err = s.retry(ctx, s.queueDeclarePassiveRetryCB, func() error {
		queue, err = s.channel.QueueDeclarePassive(
			name,
			o.Durable,
			o.AutoDelete,
			o.Exclusive,
			o.NoWait,
			amqp091.Table(o.Args),
		)
		return err
	})

	if err == nil {
		return Queue(queue), nil
	}

	ae := &amqp091.Error{}
	if errors.As(err, &ae) {
		return Queue{}, fmt.Errorf("queue %w: %w", ErrNotFound, ae)
	}
	return Queue{}, err
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
func (s *Session) QueueDelete(ctx context.Context, name string, option ...QueueDeleteOptions) (purgedMsgs int, err error) {
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

	err = s.retry(ctx, s.queueDeleteRetryCB, func() error {
		purgedMsgs, err = s.channel.QueueDelete(
			name,
			o.IfUnused,
			o.IfEmpty,
			o.NoWait,
		)
		return err
	})
	if err != nil {
		return 0, err
	}
	return purgedMsgs, nil
}

type QueueBindOptions struct {
	// When NoWait is false and the queue could not be bound, the channel will be
	// closed with an error.
	NoWait bool
	// Additional implementation specific arguments
	Args Table
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
func (s *Session) QueueBind(ctx context.Context, queueName string, routingKey string, exchange string, option ...QueueBindOptions) error {
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

	return s.retry(ctx, s.queueBindRetryCB, func() error {
		return s.channel.QueueBind(
			queueName,
			routingKey,
			exchange,
			o.NoWait,
			amqp091.Table(o.Args),
		)
	})
}

// QueueUnbind removes a binding between an exchange and queue matching the key and
// arguments.
// It is possible to send and empty string for the exchange name which means to
// unbind the queue from the default exchange.
func (s *Session) QueueUnbind(ctx context.Context, name string, routingKey string, exchange string, arg ...Table) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// default
	var option Table = nil
	if len(arg) > 0 {
		option = arg[0]
	}

	return s.retry(ctx, s.queueUnbindRetryCB, func() error {
		return s.channel.QueueUnbind(name, routingKey, exchange, amqp091.Table(option))
	})
}

type QueuePurgeOptions struct {
	// If NoWait is true, do not wait for the server response and the number of messages purged will not be meaningful.
	NoWait bool
}

// QueuePurge removes all messages from the named queue which are not waiting to be acknowledged.
// Messages that have been delivered but have not yet been acknowledged will not be removed.
// When successful, returns the number of messages purged.
func (s *Session) QueuePurge(ctx context.Context, name string, options ...QueuePurgeOptions) (int, error) {
	opt := QueuePurgeOptions{
		NoWait: false,
	}
	if len(options) > 0 {
		opt = options[0]
	}

	var (
		numPurgedMessages int = 0
		err               error
	)

	err = s.retry(ctx, s.queuePurgeRetryCB, func() error {
		numPurgedMessages, err = s.channel.QueuePurge(name, opt.NoWait)
		if err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return 0, err
	}
	return numPurgedMessages, nil
}

type ExchangeBindOptions struct {
	// When NoWait is true, do not wait for the server to confirm the binding.  If any
	// error occurs the channel will be closed.  Add a listener to NotifyClose to
	// handle these errors.
	NoWait bool

	// Optional arguments specific to the exchanges bound can also be specified.
	Args Table
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
func (s *Session) ExchangeBind(ctx context.Context, destination string, routingKey string, source string, option ...ExchangeBindOptions) error {
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

	return s.retry(ctx, s.exchangeBindRetryCB, func() error {
		return s.channel.ExchangeBind(
			destination,
			routingKey,
			source,
			o.NoWait,
			amqp091.Table(o.Args),
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
	Args Table
}

// ExchangeUnbind unbinds the destination exchange from the source exchange on the
// server by removing the routing key between them.  This is the inverse of
// ExchangeBind.  If the binding does not currently exist, an error will be
// returned.
func (s *Session) ExchangeUnbind(ctx context.Context, destination string, routingKey string, source string, option ...ExchangeUnbindOptions) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	o := ExchangeUnbindOptions{
		NoWait: false,
		Args:   nil,
	}
	if len(option) > 0 {
		o = option[0]
	}

	return s.retry(ctx, s.exchangeUnbindRetryCB, func() error {
		return s.channel.ExchangeUnbind(
			destination,
			routingKey,
			source,
			o.NoWait,
			amqp091.Table(o.Args),
		)
	})
}

/*
Qos controls how many messages or how many bytes the server will try to keep on
the network for consumers before receiving delivery acks.  The intent of Qos is
to make sure the network buffers stay full between the server and client.

With a prefetch count greater than zero, the server will deliver that many
messages to consumers before acknowledgments are received.  The server ignores
this option when consumers are started with noAck because no acknowledgments
are expected or sent.

With a prefetch size greater than zero, the server will try to keep at least
that many bytes of deliveries flushed to the network before receiving
acknowledgments from the consumers.  This option is ignored when consumers are
started with noAck.

To get round-robin behavior between consumers consuming from the same queue on
different connections, set the prefetch count to 1, and the next available
message on the server will be delivered to the next available consumer.

If your consumer work time is reasonably consistent and not much greater
than two times your network round trip time, you will see significant
throughput improvements starting with a prefetch count of 2 or slightly
greater as described by benchmarks on RabbitMQ.

http://www.rabbitmq.com/blog/2012/04/25/rabbitmq-performance-measurements-part-2/
*/
func (s *Session) Qos(ctx context.Context, prefetchCount int, prefetchSize int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.retry(ctx, s.qosRetryCB, func() error {
		// session Qos should not affect new sessions of the same connection
		return s.channel.Qos(prefetchCount, prefetchSize, false)
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
func (s *Session) Flow(ctx context.Context, active bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.retry(ctx, s.flowRetryCB, func() error {
		return s.channel.Flow(active)
	})
}

// Tx puts the channel into transaction mode on the server. All publishings and acknowledgments following this method
// will be atomically committed or rolled back for a single queue. Call either Channel.TxCommit or Channel.TxRollback
// to leave this transaction and immediately start a new transaction.
//
// The atomicity across multiple queues is not defined as queue declarations and bindings are not included in the transaction.
//
// The behavior of publishings that are delivered as mandatory or immediate while the channel is in a transaction is not defined.
//
// Once a channel has been put into transaction mode, it cannot be taken out of transaction mode.
// Use a different channel for non-transactional semantics.
func (s *Session) Tx() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.channel.Tx()
}

// TxCommit atomically commits all publishings and acknowledgments for a single queue and immediately start a new transaction.
//
// Calling this method without having called Channel.Tx is an error.
func (s *Session) TxCommit() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.channel.TxCommit()
}

// TxRollback atomically rolls back all publishings and acknowledgments for a single queue and immediately start a new transaction.
//
// Calling this method without having called Channel.Tx is an error.
func (s *Session) TxRollback() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.channel.TxRollback()
}

// Error returns all errors from the errors channel
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
		case e, ok := <-s.errors:
			if !ok {
				if err != nil {
					return err
				}
				return fmt.Errorf("errors channel %w", ErrClosed)
			}
			err = errors.Join(err, e)
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

func (s *Session) shutdownErr() error {
	return s.ctx.Err()
}

func (s *Session) slog() *slog.Logger {
	return s.log.With(
		slog.String("connection", s.conn.Name()),
		slog.String("session", s.name),
	)
}

// Flush confirms channel
func (s *Session) FlushConfirms() {
	s.mu.Lock()
	defer s.mu.Unlock()
	flush(s.confirms)
}

// FlushReturned publish	 channel
func (s *Session) FlushReturned() {
	s.mu.Lock()
	defer s.mu.Unlock()
	flush(s.returned)
}

// flush is a helper function to flush a channel
func flush[T any](c <-chan T) []T {
	var (
		slice []T
	)
	for {
		select {
		case e, ok := <-c:
			if !ok {
				return slice
			}
			slice = append(slice, e)
		default:
			return slice
		}
	}
}
