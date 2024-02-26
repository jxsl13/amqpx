package pool

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jxsl13/amqpx/logging"
)

// ConnectionPool houses the pool of RabbitMQ connections.
type ConnectionPool struct {
	// connection pool name will be added to all of its connections
	name string

	// connection url to connect to the RabbitMQ server (user, password, url, port, vhost, etc)
	url string

	heartbeat   time.Duration
	connTimeout time.Duration

	size int

	tls         *tls.Config
	connections chan *Connection

	transientID int64

	ctx    context.Context
	cancel context.CancelFunc

	log logging.Logger

	recoverCB ConnectionRecoverCallback
}

// NewConnectionPool creates a new connection pool which has a maximum size it
// can become and an idle size of connections that are always open.
func NewConnectionPool(ctx context.Context, connectUrl string, numConns int, options ...ConnectionPoolOption) (*ConnectionPool, error) {
	if numConns < 1 {
		return nil, fmt.Errorf("%w: %d", errInvalidPoolSize, numConns)
	}

	// use sane defaults
	option := connectionPoolOption{
		Name: defaultAppName(),
		Size: numConns,

		Ctx: ctx,

		ConnHeartbeatInterval: 15 * time.Second, // https://www.rabbitmq.com/heartbeats.html#false-positives
		ConnTimeout:           30 * time.Second,
		TLSConfig:             nil,

		Logger: logging.NewNoOpLogger(),

		ConnectionRecoverCallback: nil,
	}

	// apply options
	for _, o := range options {
		o(&option)
	}

	return newConnectionPoolFromOption(connectUrl, option)
}

func newConnectionPoolFromOption(connectUrl string, option connectionPoolOption) (_ *ConnectionPool, err error) {
	u, err := url.ParseRequestURI(connectUrl)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidConnectURL, err)
	}

	if option.TLSConfig != nil {
		u.Scheme = "amqps"
	}

	// decouple from parent context, in case we want to close this context ourselves.
	ctx, cancel := context.WithCancel(option.Ctx)

	cp := &ConnectionPool{
		name: option.Name,
		url:  u.String(),

		heartbeat:   option.ConnHeartbeatInterval,
		connTimeout: option.ConnTimeout,

		size:        option.Size,
		tls:         option.TLSConfig,
		connections: make(chan *Connection, option.Size),

		ctx:    ctx,
		cancel: cancel,

		log: option.Logger,

		recoverCB: option.ConnectionRecoverCallback,
	}

	cp.debug("initializing pool connections")
	defer func() {
		if err != nil {
			cp.error(err, "failed to initialize pool connections")
		} else {
			cp.info("initialized")
		}
	}()

	err = cp.initCachedConns()
	if err != nil {
		return nil, err
	}

	return cp, nil
}

func (cp *ConnectionPool) initCachedConns() error {
	for id := 0; id < cp.size; id++ {
		conn, err := cp.deriveConnection(cp.ctx, id, true)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrPoolInitializationFailed, err)
		}

		select {
		case cp.connections <- conn:
		case <-cp.ctx.Done():
			return fmt.Errorf("%w: %v", ErrPoolInitializationFailed, cp.ctx.Err())
		default:
			// should not happen
			return fmt.Errorf("%w: pool channel buffer full", ErrPoolInitializationFailed)
		}

	}
	return nil
}

func (cp *ConnectionPool) deriveConnection(ctx context.Context, id int, cached bool) (*Connection, error) {
	var name string
	if cached {
		name = fmt.Sprintf("%s-cached-connection-%d", cp.name, id)
	} else {
		name = fmt.Sprintf("%s-transient-connection-%d", cp.name, id)
	}
	return NewConnection(ctx, cp.url, name,
		ConnectionWithTimeout(cp.connTimeout),
		ConnectionWithHeartbeatInterval(cp.heartbeat),
		ConnectionWithTLS(cp.tls),
		ConnectionWithCached(cached),
		ConnectionWithLogger(cp.log),
		ConnectionWithRecoverCallback(cp.recoverCB),
	)
}

// GetConnection only returns an error upon shutdown
func (cp *ConnectionPool) GetConnection(ctx context.Context) (*Connection, error) {
	select {
	case <-cp.catchShutdown():
		return nil, fmt.Errorf("connection pool %w", ErrClosed)
	case <-ctx.Done():
		return nil, ctx.Err()
	case conn, ok := <-cp.connections:
		if !ok {
			return nil, fmt.Errorf("connection pool %w", ErrClosed)
		}
		err := conn.Recover(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to get connection: %w", err)
		}

		return conn, nil
	}
}

// GetTransientConnection may return an error when the context was cancelled before the connection could be obtained.
// Transient connections may be returned to the pool. The are closed properly upon returning.
func (cp *ConnectionPool) GetTransientConnection(ctx context.Context) (*Connection, error) {
	tID := atomic.AddInt64(&cp.transientID, 1)

	conn, err := cp.deriveConnection(ctx, int(tID), false)
	if err == nil {
		return conn, nil
	}

	// recover until context is closed
	err = conn.Recover(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get transient connection: %w", err)
	}

	return conn, nil
}

// ReturnConnection puts the connection back in the queue and flag it for error.
// This helps maintain a Round Robin on Connections and their resources.
func (cp *ConnectionPool) ReturnConnection(ctx context.Context, conn *Connection, flag bool) {
	// close transient connections
	if !conn.IsCached() {
		_ = conn.Close()
		return
	}
	conn.Flag(flag)

	if conn.IsFlagged() {
		// try to recover until context is canceled
		// if recovery fails, we put the broken connection into the pool
		_ = conn.Recover(ctx)
	}

	cp.connections <- conn
}

// Close closes the connection pool.
// Closes all connections and sessions that are currently known to the pool.
// Any new connections or session requests will return an error.
// Any returned sessions or connections will be closed properly.
func (cp *ConnectionPool) Close() {

	cp.debug("closing connection pool...")
	defer cp.info("closed")

	wg := &sync.WaitGroup{}
	wg.Add(cp.size)
	cp.cancel()

	for i := 0; i < cp.size; i++ {
		go func() {
			defer wg.Done()
			conn := <-cp.connections
			_ = conn.Close()
		}()
	}

	wg.Wait()
}

func (cp *ConnectionPool) catchShutdown() <-chan struct{} {
	return cp.ctx.Done()
}

func (cp *ConnectionPool) Name() string {
	return cp.name
}

func (cp *ConnectionPool) info(a ...any) {
	cp.log.WithField("connectionPool", cp.name).Info(a...)
}

func (cp *ConnectionPool) error(err error, a ...any) {
	cp.log.WithField("connectionPool", cp.name).WithField("error", err.Error()).Error(a...)
}

func (cp *ConnectionPool) debug(a ...any) {
	cp.log.WithField("connectionPool", cp.name).Debug(a...)
}

func (cp *ConnectionPool) Size() int {
	return cp.size
}
