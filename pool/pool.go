package pool

import (
	"context"
	"crypto/tls"
	"errors"
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

	maxSize    int
	cachedSize int

	sessionBufferSize int
	sessionAckable    bool

	tls         *tls.Config
	connections *queue.Queue
	sessions    chan *Session

	flaggedConns map[int64]bool
	connID       int64

	mu     sync.RWMutex
	ctx    context.Context
	cancel context.CancelFunc
}

// NewConnectionPool creates a new connection pool which has a maximum size it
// can become and an idle size of connections that are always open.
func NewConnectionPool(connectUrl string, maxSize int, options ...PoolOption) (*ConnectionPool, error) {
	if maxSize < 1 {
		panic("max pool size is negative or 0")
	}

	// use sane defaults
	option := poolOption{
		Name: defaultAppName(),
		Ctx:  context.Background(),

		BackoffPolicy: newDefaultBackoffPolicy(time.Second, 15*time.Second),
		CachedSize:    maxSize / 2,

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

	ctx, cancel := context.WithCancel(option.Ctx)

	cp := &ConnectionPool{
		url: u.String(),

		heartbeat:    option.ConnHeartbeatInterval,
		connTimeout:  option.ConnTimeout,
		errorBackoff: option.BackoffPolicy,

		maxSize:    maxSize,
		cachedSize: option.CachedSize,

		sessionBufferSize: option.SessionBufferSize,
		sessionAckable:    option.SessionAckable,

		tls:         option.TLSConfig,
		connections: queue.New(int64(maxSize)),
		sessions:    make(chan *Session, maxSize),

		flaggedConns: make(map[int64]bool, maxSize),
		connID:       0,

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
	maxSize := int64(cp.maxSize)
	for cp.connID = 0; cp.connID < maxSize; cp.connID++ {
		conn, err := cp.deriveConnection(cp.connID)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrPoolInitializationFailed, err)
		}

		if err = cp.connections.Put(conn); err != nil {
			return fmt.Errorf("%w: %v", ErrPoolInitializationFailed, err)
		}
	}

	for i := 0; i < cp.cachedSize; i++ {
		session, err := cp.createCachedSession(int64(i))
		if err != nil {
			return err
		}
		cp.sessions <- session
	}

	return nil
}

func (cp *ConnectionPool) deriveConnection(id int64) (*Connection, error) {
	name := fmt.Sprintf("%s-%d", cp.name, id)
	return NewConnection(cp.url, name, id,
		ConnectionContext(cp.ctx),
		ConnectionTimeout(cp.connTimeout),
		ConnectionHeartbeatInterval(cp.heartbeat),
		ConnectionWithTLS(cp.tls),
	)
}

func (cp *ConnectionPool) deriveSession(conn *Connection, id int64, cached bool) (*Session, error) {
	return NewSession(conn, id, cached,
		SessionWithContext(cp.ctx),
		SessionWithBufferSize(cp.sessionBufferSize),
	)
}

// createCacheSession allows you create a cached Session which helps wrap Amqp Session functionality.
func (cp *ConnectionPool) createCachedSession(id int64) (*Session, error) {

	// retry until we get a channel
	// or until shutdown
	for {

		// TODO: catch shutdown
		conn, err := cp.GetConnection()
		if err != nil {
			continue
		}

		session, err := cp.deriveSession(conn, id, true)
		if err != nil {
			cp.ReturnConnection(conn, true)
			continue
		}

		cp.ReturnConnection(conn, false)
		return session, nil
	}
}

// TODO: everything below here must be reimplemented and checked properly

func (cp *ConnectionPool) GetConnection() (*Connection, error) {

	conn, err := cp.getConnectionFromPool()
	if err != nil { // errors on bad data in the queue
		return nil, err
	}

	cp.verifyHealthyConnection(conn)

	return conn, nil
}

func (cp *ConnectionPool) getConnectionFromPool() (*Connection, error) {

	// Pull from the queue.
	// Pauses here indefinitely if the queue is empty.
	structs, err := cp.connections.Get(1)
	if err != nil {
		return nil, err
	}

	conn, ok := structs[0].(*Connection)
	if !ok {
		return nil, errors.New("invalid struct type found in ConnectionPool queue")
	}

	return conn, nil
}

func (cp *ConnectionPool) verifyHealthyConnection(conn *Connection) {

	healthy := true
	select {
	case <-conn.Errors():
		healthy = false
	default:
		break
	}

	flagged := cp.isConnectionFlagged(conn.ID())

	// Between these three states we do our best to determine that a connection is dead in the various lifecycles.
	if flagged || !healthy || conn.IsClosed() {
		cp.triggerConnectionRecovery(conn)
	}

	conn.PauseOnFlowControl()
}

func (cp *ConnectionPool) triggerConnectionRecovery(conn *Connection) {

	// InfiniteLoop: Stay here till we reconnect.

	for retry := 0; ; retry++ {
		// TODO: catch shutdown upon retry, use ticker

		err := conn.Connect()
		if err != nil {
			// TODO: use timer
			time.Sleep(cp.errorBackoff(retry))
			continue
		}
		break
	}

	for {
		select {
		case <-conn.Errors():
			// Flush any pending errors.
		default:
			cp.unflagConnection(conn.ID())
			return
		}
	}
}

// ReturnConnection puts the connection back in the queue and flag it for error.
// This helps maintain a Round Robin on Connections and their resources.
func (cp *ConnectionPool) ReturnConnection(conn *Connection, flag bool) {

	if flag {
		cp.flagConnection(conn.ID())
	}

	err := cp.connections.Put(conn)
	if err != nil {
		// close connection in case the queue blocks
		// returning
		conn.Close()
	}
}

// GetSessionFromPool gets a cached ackable channel from the Pool if they exist or creates a channel.
// A non-acked channel is always a transient channel.
// Blocking if Ackable is true and the cache is empty.
// If you want a transient Ackable channel (un-managed), use CreateSession directly.
func (cp *ConnectionPool) GetSession() (*Session, error) {
	select {
	case session, ok := <-cp.sessions:
		if !ok {
			return nil, ErrPoolClosed
		}
		return session, nil

	}
}

// ReturnSession returns a Session.
// If Session is not a cached channel, it is simply closed here.
// If Cache Session, we check if erred, new Session is created instead and then returned to the cache.
func (cp *ConnectionPool) ReturnSession(session *Session, erred bool) {

	// If called by user with the wrong channel don't add a non-managed channel back to the channel cache.
	if session.IsCached() {
		if erred {
			cp.reconnectSession(session) // <- blocking operation
		} else {
			session.flushConfirms()
		}

		cp.sessions <- session
		return
	}

	go func(*Session) {
		defer func() { _ = recover() }()

		session.Close()
	}(session)
}

func (cp *ConnectionPool) reconnectSession(session *Session) {

	// InfiniteLoop: Stay here till we reconnect.
	for {
		cp.verifyHealthyConnection(session.conn) // <- blocking operation

		err := session.Connect() // Creates a new channel and flushes internal buffers automatically.
		if err != nil {
			continue
		}
		break
	}
}

// GetTransientSession allows you create an unmanaged amqp Session with the help of the ConnectionPool.
func (cp *ConnectionPool) GetTransientSession(ackable bool) (*Session, error) {

	// InfiniteLoop: Stay till we have a good channel.
	for {
		conn, err := cp.GetConnection()
		if err != nil {
			continue
		}

		session, err := cp.deriveSession(conn, 0, false)
		if err != nil {
			cp.ReturnConnection(conn, true)
			continue
		}

		cp.ReturnConnection(conn, false)
		return session, nil
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

// IsConnectionFlagged checks to see if the connection has been flagged for removal.
func (cp *ConnectionPool) isConnectionFlagged(connectionID int64) bool {
	cp.mu.RLock()
	defer cp.mu.RUnlock()
	if flagged, ok := cp.flaggedConns[connectionID]; ok {
		return flagged
	}

	return false
}

// Shutdown closes all connections in the ConnectionPool and resets the Pool to pre-initialized state.
func (cp *ConnectionPool) Shutdown() {

	if cp == nil {
		return
	}

	wg := &sync.WaitGroup{}
SessionClose:
	for {
		select {
		case session := <-cp.sessions:
			wg.Add(1)
			go func(*Session) {
				defer wg.Done()
				session.Close()
			}(session)

		default:
			break SessionClose
		}
	}
	wg.Wait()

	for !cp.connections.Empty() {
		items := cp.connections.Dispose()

		for _, item := range items {
			conn := item.(*Connection)
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

func (cp *ConnectionPool) isShutdown() bool {
	select {
	case <-cp.ctx.Done():
		return true
	default:
		return false
	}
}
