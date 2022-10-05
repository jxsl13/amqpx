package pool

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Workiva/go-datastructures/queue"
	"github.com/jxsl13/amqpx/logging"
)

// ConnectionPool houses the pool of RabbitMQ connections.
type ConnectionPool struct {
	name string
	url  string

	heartbeat   time.Duration
	connTimeout time.Duration

	size int

	tls         *tls.Config
	connections *queue.Queue

	transientID atomic.Int64

	ctx    context.Context
	cancel context.CancelFunc

	log logging.Logger
}

// NewConnectionPool creates a new connection pool which has a maximum size it
// can become and an idle size of connections that are always open.
func NewConnectionPool(connectUrl string, size int, options ...ConnectionPoolOption) (*ConnectionPool, error) {
	if size < 1 {
		panic("max pool size is negative or 0")
	}

	// use sane defaults
	option := connectionPoolOption{
		Name: defaultAppName(),
		Size: size,

		Ctx: context.Background(),

		ConnHeartbeatInterval: 15 * time.Second, // https://www.rabbitmq.com/heartbeats.html#false-positives
		ConnTimeout:           30 * time.Second,
		TLSConfig:             nil,

		Logger: logging.NewNoOpLogger(),
	}

	// apply options
	for _, o := range options {
		o(&option)
	}

	return newConnectionPoolFromOption(connectUrl, option)
}

func newConnectionPoolFromOption(connectUrl string, option connectionPoolOption) (*ConnectionPool, error) {
	u, err := url.Parse(connectUrl)
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
		connections: queue.New(int64(option.Size)),

		ctx:    ctx,
		cancel: cancel,

		log: option.Logger,
	}

	err = cp.initCachedConns()
	if err != nil {
		return nil, err
	}

	return cp, nil
}

func (cp *ConnectionPool) initCachedConns() error {
	for id := 0; id < cp.size; id++ {
		conn, err := cp.deriveConnection(id, true)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrPoolInitializationFailed, err)
		}

		if err = cp.connections.Put(conn); err != nil {
			return fmt.Errorf("%w: %v", ErrPoolInitializationFailed, err)
		}
	}
	return nil
}

func (cp *ConnectionPool) deriveConnection(id int, cached bool) (*Connection, error) {
	name := fmt.Sprintf("%s-%d", cp.name, id)
	return NewConnection(cp.url, name, int64(id),
		ConnectionWithContext(cp.ctx),
		ConnectionWithTimeout(cp.connTimeout),
		ConnectionWithHeartbeatInterval(cp.heartbeat),
		ConnectionWithTLS(cp.tls),
		ConnectionWithCached(cached),
	)
}

// GetConnection only returns an error upon shutdown
func (cp *ConnectionPool) GetConnection() (*Connection, error) {

	conn, err := cp.getConnectionFromPool()
	if err != nil {
		return nil, err
	}

	err = conn.Recover()
	if err != nil {
		return nil, fmt.Errorf("failed to get connection: %w", err)
	}

	return conn, nil
}

// GetTransientConnection may return an error when the context was cancelled before the connection could be obtained.
// Transient connections may be returned to the pool. The are closed properly upon returning.
func (cp *ConnectionPool) GetTransientConnection(ctx context.Context) (*Connection, error) {

	tID := cp.transientID.Add(1)

	name := fmt.Sprintf("%s-transient-%d", cp.name, tID)
	conn, err := NewConnection(cp.url, name, tID,
		ConnectionWithContext(ctx),
		ConnectionWithTimeout(cp.connTimeout),
		ConnectionWithHeartbeatInterval(cp.heartbeat),
		ConnectionWithTLS(cp.tls),
		ConnectionWithCached(false),
	)
	if err == nil {
		return conn, nil
	}

	// recover until context is closed
	err = conn.Recover()
	if err != nil {
		return nil, fmt.Errorf("failed to get transient connection: %w", err)
	}

	return conn, nil
}

func (cp *ConnectionPool) getConnectionFromPool() (*Connection, error) {

	// Pull from the queue.
	// Pauses here indefinitely if the queue is empty.
	objects, err := cp.connections.Get(1)
	if err != nil {
		return nil, fmt.Errorf("connection pool %w: %v", ErrClosed, err)
	}

	conn, ok := objects[0].(*Connection)
	if !ok {
		panic("invalid type in queue")
	}
	return conn, nil
}

// ReturnConnection puts the connection back in the queue and flag it for error.
// This helps maintain a Round Robin on Connections and their resources.
func (cp *ConnectionPool) ReturnConnection(conn *Connection, flag bool) {
	if flag {
		conn.Flag(flag)
	}

	// close transient connections
	if !conn.IsCached() {
		conn.Close()
	}

	err := cp.connections.Put(conn)
	if err != nil {
		// queue was disposed of,
		// indicating pool shutdown
		// -> close connection upon pool shutdown
		conn.Close()
	}
}

// Close closes the connection pool.
// Closes all connections and sessions that are currently known to the pool.
// Any new connections or session requests will return an error.
// Any returned sessions or connections will be closed properly.
func (cp *ConnectionPool) Close() {

	wg := &sync.WaitGroup{}

	// close all connections
	for !cp.connections.Empty() {
		items := cp.connections.Dispose()

		for _, item := range items {
			conn, ok := item.(*Connection)
			if !ok {
				panic("item in connection queue is not a connection")
			}

			wg.Add(1)
			go func(c *Connection) {
				defer wg.Done()
				c.Close()
			}(conn)
		}
	}
	wg.Wait()
}
