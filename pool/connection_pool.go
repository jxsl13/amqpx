package pool

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/jxsl13/amqpx/internal/contextutils"
	"github.com/jxsl13/amqpx/logging"
	"github.com/jxsl13/amqpx/types"
)

// ConnectionPool houses the pool of RabbitMQ connections.
type ConnectionPool struct {
	// connection pool name will be added to all of its connections
	name string

	// connection url to connect to the RabbitMQ server (user, password, url, port, vhost, etc)
	url string

	heartbeat   time.Duration
	connTimeout time.Duration

	capacity int

	tls *tls.Config

	ctx    context.Context
	cancel context.CancelFunc

	log logging.Logger

	recoverCB types.ConnectionRecoverCallback

	connections chan *types.Connection

	mu                  sync.Mutex
	transientID         int64
	concurrentTransient int
}

// NewConnectionPool creates a new connection pool which has a maximum size it
// can become and an idle size of connections that are always open.
func NewConnectionPool(ctx context.Context, connectUrl string, numConns int, options ...ConnectionPoolOption) (*ConnectionPool, error) {
	if numConns < 1 {
		return nil, fmt.Errorf("%w: %d", errInvalidPoolSize, numConns)
	}

	// use sane defaults
	option := connectionPoolOption{
		Name:     defaultAppName(),
		Capacity: numConns,

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
		return nil, fmt.Errorf("%w: %v", types.ErrInvalidConnectURL, err)
	}

	if option.TLSConfig != nil {
		u.Scheme = "amqps"
	}

	// decouple from parent context, in case we want to close this context ourselves.
	ctx, cc := context.WithCancelCause(option.Ctx)
	cancel := contextutils.ToCancelFunc(fmt.Errorf("connection pool %w", types.ErrClosed), cc)

	cp := &ConnectionPool{
		name: option.Name,
		url:  u.String(),

		heartbeat:   option.ConnHeartbeatInterval,
		connTimeout: option.ConnTimeout,

		capacity:    option.Capacity,
		tls:         option.TLSConfig,
		connections: make(chan *types.Connection, option.Capacity),

		ctx:    ctx,
		cancel: cancel,

		log: option.Logger,

		recoverCB: option.ConnectionRecoverCallback,
	}

	cp.debug("initializing pool connections...")
	defer func() {
		if err != nil {
			cp.error(err, "failed to initialize pool connections")
		} else {
			cp.info("initialized pool connections")
		}
	}()

	err = cp.initCachedConns()
	if err != nil {
		return nil, err
	}

	return cp, nil
}

func (cp *ConnectionPool) initCachedConns() error {
	for id := int64(0); id < int64(cp.capacity); id++ {
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

func (cp *ConnectionPool) deriveConnection(ctx context.Context, id int64, cached bool) (*types.Connection, error) {
	var name string
	if cached {
		name = fmt.Sprintf("%s-cached-connection-%d", cp.name, id)
	} else {
		name = fmt.Sprintf("%s-transient-connection-%d", cp.name, id)
	}
	return types.NewConnection(ctx, cp.url, name,
		types.ConnectionWithTimeout(cp.connTimeout),
		types.ConnectionWithHeartbeatInterval(cp.heartbeat),
		types.ConnectionWithTLS(cp.tls),
		types.ConnectionWithCached(cached),
		types.ConnectionWithLogger(cp.log),
		types.ConnectionWithRecoverCallback(cp.recoverCB),
	)
}

// GetConnection only returns an error upon shutdown
func (cp *ConnectionPool) GetConnection(ctx context.Context) (conn *types.Connection, err error) {
	select {
	case conn, ok := <-cp.connections:
		if !ok {
			return nil, fmt.Errorf("connection pool %w", types.ErrClosed)
		}

		// recovery may fail, that's why we MUST check for errors
		// and return the connection back to the pool in case that the recovery failed
		// due to e.g. the pool being closed, the context being canceled, etc.
		defer func() {
			if err != nil {
				cp.ReturnConnection(conn, err)
			}
		}()

		err = conn.Recover(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to get connection: %w", err)
		}

		return conn, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-cp.catchShutdown():
		return nil, fmt.Errorf("connection pool %w", types.ErrClosed)
	}
}

func (cp *ConnectionPool) nextTransientID() int64 {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	cp.transientID++
	return cp.transientID
}

// GetTransientConnection may return an error when the context was cancelled before the connection could be obtained.
// Transient connections may be returned to the pool. The are closed properly upon returning.
func (cp *ConnectionPool) GetTransientConnection(ctx context.Context) (conn *types.Connection, err error) {
	conn, err = cp.deriveConnection(ctx, cp.nextTransientID(), false)
	if err == nil {
		return conn, nil
	}
	defer func() {
		if err != nil {
			_ = conn.Close()
		}
	}()

	// recover until context is closed
	err = conn.Recover(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get transient connection: %w", err)
	}

	return conn, nil
}

// ReturnConnection puts the connection back in the queue and flag it for error.
// This helps maintain a Round Robin on Connections and their resources.
// If the connection is flagged, it will be recovered and returned to the pool.
// If the context is canceled, the connection will be immediately returned to the pool
// without any recovery attempt.
func (cp *ConnectionPool) ReturnConnection(conn *types.Connection, err error) {
	// close transient connections
	if !conn.IsCached() {
		_ = conn.Close()
		return
	}
	conn.Flag(err)

	select {
	case cp.connections <- conn:
	default:
		panic("connection pool connections buffer full: not supposed to happen")
	}
}

// Close closes the connection pool.
// Closes all connections and sessions that are currently known to the pool.
// Any new connections or session requests will return an error.
// Any returned sessions or connections will be closed properly.
func (cp *ConnectionPool) Close() {

	cp.debug("closing connection pool...")
	defer cp.info("closed")

	wg := &sync.WaitGroup{}
	wg.Add(cp.capacity)
	cp.cancel()

	for i := 0; i < cp.capacity; i++ {
		go func() {
			defer wg.Done()
			conn := <-cp.connections
			_ = conn.Close()
		}()
	}

	wg.Wait()
}

// StatTransientActive returns the number of active transient connections.
func (cp *ConnectionPool) StatTransientActive() int {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	return cp.concurrentTransient
}

// StatCachedActive returns the number of active cached connections.
func (cp *ConnectionPool) StatCachedActive() int {
	return cp.capacity - len(cp.connections)
}

// Size returns the number of idle cached connections.
func (cp *ConnectionPool) Size() int {
	return len(cp.connections)
}

// Capacity is the capacity of the cached connection pool without any transient connections.
// It is the initial number of connections that were created for this connection pool.
func (cp *ConnectionPool) Capacity() int {
	return cp.capacity
}

func (cp *ConnectionPool) catchShutdown() <-chan struct{} {
	return cp.ctx.Done()
}

func (cp *ConnectionPool) Name() string {
	return cp.name
}

func (cp *ConnectionPool) info(a ...any) {
	cp.log.WithField("connection_pool", cp.name).Info(a...)
}

func (cp *ConnectionPool) error(err error, a ...any) {
	cp.log.WithField("connection_pool", cp.name).WithField("error", err.Error()).Error(a...)
}

func (cp *ConnectionPool) debug(a ...any) {
	cp.log.WithField("connection_pool", cp.name).Debug(a...)
}
