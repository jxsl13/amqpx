package pool

import (
	"context"
	"fmt"
	"sync"
)

type SessionPool struct {
	pool *ConnectionPool

	sessionId int64

	size        int
	bufferSize  int
	confirmable bool
	sessions    chan *Session

	ctx    context.Context
	cancel context.CancelFunc
}

func NewSessionPool(pool *ConnectionPool, size int, options ...SessionPoolOption) (*SessionPool, error) {
	if size < 1 {
		panic("max pool size is negative or 0")
	}

	// use sane defaults
	option := sessionPoolOption{
		Size:        size,
		Confirmable: false,
		BufferSize:  1,        // fault tolerance over throughput
		Ctx:         pool.ctx, // derive context from parent
	}

	for _, o := range options {
		o(&option)
	}

	return newSessionPoolFromOption(pool, option)
}

func newSessionPoolFromOption(pool *ConnectionPool, option sessionPoolOption) (*SessionPool, error) {
	// decouple from parent context, in case we want to close this context ourselves.
	ctx, cancel := context.WithCancel(option.Ctx)

	sessionPool := &SessionPool{
		pool:      pool,
		sessionId: 1, // transient sessions get an id of 0 and pooled sessions are incremented, starting from 1

		size:        option.Size,
		bufferSize:  option.BufferSize,
		confirmable: option.Confirmable,
		sessions:    make(chan *Session, option.Size),

		ctx:    ctx,
		cancel: cancel,
	}

	err := sessionPool.initSessions()
	if err != nil {
		return nil, err
	}

	return sessionPool, nil
}

// GetSession gets a pooled session.
// blocks until a session is acquired from the pool.
func (sp *SessionPool) GetSession() (*Session, error) {
	select {
	case <-sp.catchShutdown():
		return nil, ErrClosed
	case session, ok := <-sp.sessions:
		if !ok {
			return nil, fmt.Errorf("failed to get session: %w", ErrClosed)
		}
		return session, nil
	}
}

// ReturnSession returns a Session.
// If Session is not a cached channel, it is simply closed here.
// If Cache Session, we check if erred, new Session is created instead and then returned to the cache.
func (sp *SessionPool) ReturnSession(session *Session, erred bool) {

	// don't ass non-managed sessions back to the channel
	if !session.IsCached() {
		session.Close()
		return
	}

	if erred {
		err := sp.recoverSession(session)
		if err != nil {
			// error is only returned on shutdown,
			// don't recover upon shutdown
			return
		}
	} else {
		// healthy sessions may contain pending confirmation messages
		// cleanup confirmations from previous session usage
		session.flushConfirms()
	}

	select {
	case <-sp.catchShutdown():
		session.Close()
	case sp.sessions <- session:
	}
}

func (sp *SessionPool) recoverSession(session *Session) error {

	// tries to recover session forever
	for {
		err := session.conn.Recover() // recovers connection with a backoff mechanism
		if err != nil {
			// upon shutdown this will fail
			return fmt.Errorf("failed to recover session: %w", err)
		}

		// no backoff upon retry, because Recover already retries
		// with a backoff. Sessions should be instantly created on a healthy connection
		err = session.Connect() // Creates a new channel and flushes internal buffers automatically.
		if err != nil {
			continue
		}
		break
	}

	return nil
}

func (sp *SessionPool) catchShutdown() <-chan struct{} {
	return sp.ctx.Done()
}

// Closes the session pool with all of its sessions
func (sp *SessionPool) Close() {

	wg := &sync.WaitGroup{}

	// close all sessions
SessionClose:
	for {
		select {
		// flush sessions channel
		case session := <-sp.sessions:
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
}

func (sp *SessionPool) initSessions() error {
	for i := 0; i < sp.size; i++ {
		session, err := sp.initSession(i)
		if err != nil {
			return err
		}
		sp.sessions <- session
	}
	return nil
}

// initSession allows you create a pooled Session.
func (sp *SessionPool) initSession(id int) (*Session, error) {

	// retry until we get a channel
	// or until shutdown
	for {
		conn, err := sp.pool.GetConnection()
		if err != nil {
			// error is only returned upon shutdown
			return nil, err
		}

		session, err := sp.deriveSession(conn, id, true)
		if err != nil {
			sp.pool.ReturnConnection(conn, true)
			continue
		}

		sp.pool.ReturnConnection(conn, false)
		return session, nil
	}
}

func (sp *SessionPool) deriveSession(conn *Connection, id int, cached bool) (*Session, error) {
	return NewSession(conn, int64(id),
		SessionWithContext(sp.ctx),
		SessionWithBufferSize(sp.size),
		SessionWithCached(cached),
		SessionWithConfirms(sp.confirmable),
	)
}
