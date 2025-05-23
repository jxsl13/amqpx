package types

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"net/url"

	"github.com/jxsl13/amqpx/internal/contextutils"
	"github.com/jxsl13/amqpx/internal/errorutils"
	"github.com/jxsl13/amqpx/internal/timerutils"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Connection is an internal representation of amqp.Connection.
type Connection struct {
	// connection url (user ,password, host, port, vhost, etc)
	url  string
	addr string

	name string

	// indicates that the connection is part of a connection pool.
	cached bool
	// if set to true, the connection is marked as broken, indicating the connection must be recovered
	flagged bool

	tls *tls.Config

	// underlying amqp connection
	conn         *amqp.Connection
	lastConnLoss time.Time

	// backoff policy
	errorBackoff BackoffFunc

	heartbeat time.Duration

	// connection timeout is only used for the inital connection
	// recovering connections are recovered as long as the calling context
	// is not canceled
	connTimeout time.Duration

	errors chan *amqp.Error
	// flow control messages from rabbitmq
	blocking chan amqp.Blocking

	mu     sync.Mutex
	ctx    context.Context
	cancel context.CancelFunc

	log *slog.Logger

	recoverCB ConnectionRecoverCallback
}

// NewConnection creates a connection wrapper.
// name: unique connection name
func NewConnection(ctx context.Context, connectUrl, name string, options ...ConnectionOption) (*Connection, error) {
	// use sane defaults
	option := connectionOption{
		Logger:            slog.New(slog.DiscardHandler),
		Cached:            false,
		HeartbeatInterval: 15 * time.Second,
		ConnectionTimeout: 30 * time.Second,
		BackoffPolicy:     NewBackoffPolicy(time.Second, 15*time.Second),
		Ctx:               ctx,
		RecoverCallback:   nil,
	}

	// apply options
	for _, o := range options {
		o(&option)
	}

	u, err := url.ParseRequestURI(connectUrl)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidConnectURL, err)
	}

	if option.TLSConfig != nil {
		u.Scheme = "amqps"
	}

	// we derive a new context from the parent one in order to
	// be able to close it without affecting the parent
	cCtx, cc := context.WithCancelCause(option.Ctx)
	cancel := contextutils.ToCancelFunc(fmt.Errorf("connection %w", ErrClosed), cc)

	conn := &Connection{
		url:     u.String(),
		addr:    u.Host,
		name:    name,
		cached:  option.Cached,
		flagged: false,
		tls:     option.TLSConfig,

		conn: nil, // will be initialized below

		heartbeat:    option.HeartbeatInterval,
		connTimeout:  option.ConnectionTimeout,
		errorBackoff: option.BackoffPolicy,

		errors:   make(chan *amqp.Error, 10),
		blocking: make(chan amqp.Blocking, 10),

		ctx:    cCtx,
		cancel: cancel,

		log:          option.Logger,
		lastConnLoss: time.Now(),

		recoverCB: option.RecoverCallback,
	}

	err = conn.Connect(ctx)
	if err == nil {
		return conn, nil
	}

	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("connection failed: %w", ctx.Err())
	default:
		// continue
	}

	if !errorutils.Recoverable(err) {
		return nil, err
	}

	err = conn.Recover(ctx)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func (ch *Connection) Close() (err error) {
	if ch == nil {
		return nil
	}

	log := ch.clog()

	ch.mu.Lock()
	defer ch.mu.Unlock()

	log.Debug("closing connection...")
	defer func() {
		if err != nil {
			log.Error(fmt.Sprintf("failed to close connection: %s", err.Error()))
		} else {
			log.Info("closed")
		}
	}()

	ch.cancel() // close derived context

	if !ch.isClosed() {
		return ch.conn.Close() // close internal channel
	}

	return nil
}

// Flag flags the connection as broken which must be recovered.
// A flagged connection implies a closed connection.
// Flagging of a connectioncan only be undone by Recover-ing the connection.
func (ch *Connection) Flag(err error) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	flagged := err != nil && errorutils.Recoverable(err)

	if !ch.flagged && flagged {
		ch.flagged = flagged
	}
}

func (ch *Connection) IsFlagged() bool {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	return ch.flagged
}

// Connect tries to connect (or reconnect)
// Does not block indefinitely, but returns an error
// upon connection failure.
func (ch *Connection) Connect(ctx context.Context) error {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	return ch.connect(ctx)
}

func (ch *Connection) connect(ctx context.Context) error {
	log := ch.clog()

	// not closed, close before reconnecting
	if !ch.isClosed() {
		// ignore errors
		err := ch.conn.Close()
		if err != nil {
			log.Error(fmt.Sprintf("failed to close connection before reconnecting: %s", err.Error()))
		}
	}

	log.Debug("connecting...")
	amqpConn, err := amqp.DialConfig(ch.url,
		amqp.Config{
			Heartbeat:       ch.heartbeat,
			Dial:            defaultDial(ctx, ch.connTimeout),
			TLSClientConfig: ch.tls.Clone(),
			Properties: amqp.Table{
				"connection_name": ch.name,
			},
		})
	if err != nil {
		// wrap the underlying amqp091 error
		return fmt.Errorf("%v: %w", ErrConnectionFailed, err)
	}

	// override upon reconnect
	ch.conn = amqpConn
	ch.errors = make(chan *amqp.Error, 10)
	ch.blocking = make(chan amqp.Blocking, 10)

	// ch.Errors is closed by streadway/amqp in some scenarios :(
	ch.conn.NotifyClose(ch.errors)
	ch.conn.NotifyBlocked(ch.blocking)

	log.Info("connected")
	return nil
}

func (ch *Connection) BlockingFlowControl() <-chan amqp.Blocking {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	return ch.blocking
}

func (ch *Connection) IsClosed() bool {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	return ch.isClosed()
}

// returns true in case the
// underlying connection is either nil or closed
// non-locking: should only be used internally
func (ch *Connection) isClosed() bool {

	// connection closed 							-> cannot access it
	// connection not closed but shutdown triggered -> is closed
	return ch.conn == nil || ch.conn.IsClosed()
}

// Error returns the first error from the errors channel
// and flushes all other pending errors from the channel
// In case that there are no errors, nil is returned.
func (ch *Connection) Error() error {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	return ch.error()
}

// not threadsafe
func (ch *Connection) error() error {
	var (
		err error = nil
	)
	for {
		select {
		case <-ch.catchShutdown():
			return ch.shutdownErr()
		case e, ok := <-ch.errors:
			if !ok {
				// because the amqp library might close this
				// channel, we assume that closing was done due to
				// a library error
				return fmt.Errorf("connection and errors channel %w", ErrClosed)
			}
			// only overwrite with the first error
			err = errors.Join(err, e)
		default:
			// return err after flushing errors channel
			return err
		}
	}
}

// Recover tries to recover the connection until
// a shutdown occurs via context cancelation or until the passed context is closed.
func (ch *Connection) Recover(ctx context.Context) error {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	return ch.recover(ctx)
}

func (ch *Connection) recover(ctx context.Context) (err error) {

	select {
	case <-ctx.Done():
		return fmt.Errorf("connection recovery failed: %w", ctx.Err())
	case <-ch.catchShutdown():
		return fmt.Errorf("connection recovery failed: %w", ch.shutdownErr())
	default:
		// try recovering after checking if contexts are still valid
	}

	healthy := !ch.flagged && ch.error() == nil && !ch.isClosed()

	if healthy {
		return nil
	}

	var (
		timer   = time.NewTimer(0)
		drained = false
		log     = ch.clog()
	)
	defer timerutils.CloseTimer(timer, &drained)

	log.Info("recovering connection")
	for try := 0; ; try++ {
		ch.lastConnLoss = time.Now()
		err := ch.connect(ctx)
		if err == nil {
			// connection established successfully
			break
		}

		select {
		case <-ch.catchShutdown():
			// catch shutdown signal
			return fmt.Errorf("connection recovery failed: %w", ch.shutdownErr())
		case <-ctx.Done():
			// catch context cancelation
			return fmt.Errorf("connection recovery failed: %w", ctx.Err())
		default:

			// context cancelation is not handle din the recoverable function
			if !errorutils.Recoverable(err) {
				return err
			}

			if ch.recoverCB != nil {
				// allow a user to hook into the recovery process
				// in order to notify about the recovery process
				ch.recoverCB(ch.name, try, err)
			}
		}

		// reset to exponential backoff
		timerutils.ResetTimer(timer, ch.errorBackoff(try), &drained)

		select {
		case <-ch.catchShutdown():
			// catch shutdown signal
			return fmt.Errorf("connection recovery failed: %w", ch.shutdownErr())
		case <-ctx.Done():
			// catch context cancelation
			return fmt.Errorf("connection recovery failed: %w", ctx.Err())
		case <-timer.C:
			drained = true
			// retry after sleep
			continue
		}
	}

	// flagged connections can only
	// be unflagged via recovery
	ch.flagged = false

	log.Info("recovered connection")
	return nil
}

func (c *Connection) channel() (*amqp.Channel, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn.Channel()
}

// IsCached returns true in case this session is supposed to be returned to a session pool.
func (c *Connection) IsCached() bool {
	return c.cached
}

// Name returns the name of the connection
func (c *Connection) Name() string {
	return c.name
}

func (ch *Connection) catchShutdown() <-chan struct{} {
	return ch.ctx.Done()
}

func (ch *Connection) shutdownErr() error {
	return ch.ctx.Err()
}

func (ch *Connection) clog() *slog.Logger {
	return ch.log.With(
		slog.String("connection", ch.name),
		slog.String("address", ch.addr),
	)
}
