package pool

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"
	"time"

	"net/url"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Connection is an internal representation of amqp.Connection.
type Connection struct {
	url  string
	name string
	tls  *tls.Config

	id   uint64
	conn *amqp.Connection

	heartbeat   time.Duration
	connTimeout time.Duration

	errors   chan *amqp.Error
	blockers chan amqp.Blocking

	mu     sync.Mutex
	ctx    context.Context
	cancel context.CancelFunc
}

// NewConnection creates a connection wrapper.
func NewConnection(connectUrl, name string, id uint64, options ...ConnectionOption) (*Connection, error) {
	// use sane defaults
	option := connectionOption{
		HeartbeatInterval: 30 * time.Second,
		ConnectionTimeout: 60 * time.Second,
		Ctx:               context.Background(),
	}

	// apply options
	for _, o := range options {
		o(&option)
	}

	u, err := url.Parse(connectUrl)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidConnectURL, err)
	}

	if option.TLSConfig != nil {
		u.Scheme = "amqps"
	}

	// we derive a new context from the parent one in order to
	// be able to close it without affecting the parent
	ctx, cancel := context.WithCancel(option.Ctx)

	conn := &Connection{
		url:  u.String(),
		name: name,
		tls:  option.TLSConfig,

		id:   id,
		conn: nil, // will be initialized below

		heartbeat:   option.HeartbeatInterval,
		connTimeout: option.ConnectionTimeout,

		errors:   make(chan *amqp.Error, 10),
		blockers: make(chan amqp.Blocking, 10),

		ctx:    ctx,
		cancel: cancel,
	}

	err = conn.Connect()
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// Connect tries to connect (or reconnect)
func (ch *Connection) Connect() error {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	// not closed, reuse
	if !ch.isClosed() {
		return nil
	}

	amqpConn, err := amqp.DialConfig(ch.url,
		amqp.Config{
			Heartbeat:       ch.heartbeat,
			Dial:            amqp.DefaultDial(ch.connTimeout),
			TLSClientConfig: ch.tls,
			Properties: amqp.Table{
				"connection_name": ch.name,
			},
		})
	if err != nil {
		return fmt.Errorf("%w: %v", ErrConnectionFailed, err)
	}

	// override upon reconnect
	ch.conn = amqpConn
	ch.errors = make(chan *amqp.Error, 10)
	ch.blockers = make(chan amqp.Blocking, 10)

	// ch.Errors is closed by streadway/amqp in some scenarios :(
	ch.conn.NotifyClose(ch.errors)
	ch.conn.NotifyBlocked(ch.blockers)

	return nil
}

// not atomic
func (ch *Connection) isClosed() bool {
	return ch.conn == nil || ch.conn.IsClosed()
}

// PauseOnFlowControl allows you to wait and sleep while receiving flow control messages.
// Sleeps for one second, repeatedly until the blocking has stopped.
// Such messages will most likely be received when the broker hits its memory or disk limits.
func (ch *Connection) PauseOnFlowControl() {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	timer := time.NewTimer(time.Second)
	defer func() {
		if !timer.Stop() {
			<-timer.C
		}
	}()

	for {
		if ch.isClosed() || ch.isShutdown() {
			return
		}

		select {
		case blocker := <-ch.blockers: // Check for flow control issues.
			if !blocker.Active {
				return
			}

			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(time.Second)

			select {
			case <-ch.catchShutdown():
				return
			case <-timer.C:
				continue
			}

		default:
			return
		}
	}
}

func (ch *Connection) IsClosed() bool {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	// connection closed 							-> cannot access it
	// connection not closed but shutdown triggered -> is closed
	return ch.conn.IsClosed() || ch.isShutdown()
}

func (ch *Connection) Close() error {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	ch.cancel()            // close derived context
	return ch.conn.Close() // close internal channel
}

func (ch *Connection) catchShutdown() <-chan struct{} {
	return ch.ctx.Done()
}

func (ch *Connection) isShutdown() bool {
	select {
	case <-ch.ctx.Done():
		return true
	default:
		return false
	}
}
