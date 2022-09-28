package pool

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/Workiva/go-datastructures/queue"
)

// ConnectionPool houses the pool of RabbitMQ connections.
type ConnectionPool struct {
	name string
	url  string

	heartbeat    time.Duration
	connTimeout  time.Duration
	errorBackoff BackoffFunc

	size int

	tls         *tls.Config
	connections *queue.Queue

	flaggedConns map[int64]bool
	connID       int

	mu     sync.RWMutex
	ctx    context.Context
	cancel context.CancelFunc
}

// NewConnectionPool creates a new connection pool which has a maximum size it
// can become and an idle size of connections that are always open.
func NewConnectionPool(connectUrl string, size int, options ...PoolOption) (*ConnectionPool, error) {
	if size < 1 {
		panic("max pool size is negative or 0")
	}

	// use sane defaults
	option := poolOption{
		Name: defaultAppName(),
		Size: size,

		Ctx: context.Background(),

		BackoffPolicy: newDefaultBackoffPolicy(time.Second, 15*time.Second),

		ConnHeartbeatInterval: 30 * time.Second,
		ConnTimeout:           60 * time.Second,
		TLSConfig:             nil,

		SessionAckable:    false,
		SessionBufferSize: 1, // fault tolerance over throughput
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

	// decouple from parent context, in case we want to close this context ourselves.
	ctx, cancel := context.WithCancel(option.Ctx)

	cp := &ConnectionPool{
		name: option.Name,
		url:  u.String(),

		heartbeat:    option.ConnHeartbeatInterval,
		connTimeout:  option.ConnTimeout,
		errorBackoff: option.BackoffPolicy,

		size:        option.Size,
		tls:         option.TLSConfig,
		connections: queue.New(int64(size)),

		flaggedConns: make(map[int64]bool, size),
		connID:       1, // connections ids that are not transient start from 1 - n, transient are always 0

		ctx:    ctx,
		cancel: cancel,
	}

	err = cp.initConns()
	if err != nil {
		return nil, err
	}

	return cp, nil
}

func (cp *ConnectionPool) initConns() error {
	for cp.connID = 0; cp.connID < cp.size; cp.connID++ {
		conn, err := cp.deriveConnection(cp.connID)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrPoolInitializationFailed, err)
		}

		if err = cp.connections.Put(conn); err != nil {
			return fmt.Errorf("%w: %v", ErrPoolInitializationFailed, err)
		}
	}
	return nil
}

func (cp *ConnectionPool) deriveConnection(id int) (*Connection, error) {
	name := fmt.Sprintf("%s-%d", cp.name, id)
	return NewConnection(cp.url, name, int64(id),
		ConnectionContext(cp.ctx),
		ConnectionTimeout(cp.connTimeout),
		ConnectionHeartbeatInterval(cp.heartbeat),
		ConnectionWithTLS(cp.tls),
	)
}

func (cp *ConnectionPool) GetConnection() (*Connection, error) {

	conn, err := cp.getConnectionFromPool()
	if err != nil {
		return nil, err
	}

	err = cp.healConnection(conn)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection: %w", err)
	}

	return conn, nil
}

func (cp *ConnectionPool) getConnectionFromPool() (*Connection, error) {

	// Pull from the queue.
	// Pauses here indefinitely if the queue is empty.
	objects, err := cp.connections.Get(1)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrPoolClosed, err)
	}

	conn, ok := objects[0].(*Connection)
	if !ok {
		panic("invalid type in queue")
	}
	return conn, nil
}

func (cp *ConnectionPool) healConnection(conn *Connection) error {

	healthy := conn.Error() == nil

	flagged := cp.isFlagged(conn.ID())

	// Between these three states we do our best to determine that a connection is dead in the various lifecycles.
	if flagged || !healthy || conn.IsClosed() {
		return cp.recoverConnection(conn)
	}

	conn.PauseOnFlowControl()
	return nil
}

func (cp *ConnectionPool) recoverConnection(conn *Connection) error {
	timer := time.NewTimer(0)
	if !timer.Stop() {
		<-timer.C
	}
	defer func() {
		if !timer.Stop() {
			<-timer.C
		}
	}()

	for retry := 0; ; retry++ {
		err := conn.Connect()
		if err != nil {
			// reset to exponential backoff
			timer.Reset(cp.errorBackoff(retry))
			select {
			case <-cp.catchShutdown():
				// catch shutdown signal
				return fmt.Errorf("connection recovery failed: %w", ErrPoolClosed)
			case <-timer.C:
				if !timer.Stop() {
					<-timer.C
				}
				// retry after sleep
				continue
			}
		}

		// connection established successfully
		break
	}

	cp.unflagConnection(conn.ID())
	return nil
}

// ReturnConnection puts the connection back in the queue and flag it for error.
// This helps maintain a Round Robin on Connections and their resources.
func (cp *ConnectionPool) ReturnConnection(conn *Connection, flag bool) {

	if flag {
		cp.flagConnection(conn.ID())
	}

	err := cp.connections.Put(conn)
	if err != nil {
		// queue was disposed of,
		// indicating pool shutdown
		// -> close connection upon pool shutdown
		conn.Close()
	}
}

// UnflagConnection flags that connection as usable in the future.
func (cp *ConnectionPool) unflagConnection(connectionID int64) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	cp.flaggedConns[connectionID] = false
}

// FlagConnection flags that connection as non-usable in the future.
func (cp *ConnectionPool) flagConnection(connectionID int64) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	cp.flaggedConns[connectionID] = true
}

// isFlagged checks to see if the connection has been flagged for removal.
func (cp *ConnectionPool) isFlagged(connectionID int64) bool {
	cp.mu.RLock()
	defer cp.mu.RUnlock()
	if flagged, ok := cp.flaggedConns[connectionID]; ok {
		return flagged
	}

	return false
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

func (cp *ConnectionPool) catchShutdown() <-chan struct{} {
	return cp.ctx.Done()
}
