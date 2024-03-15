package pool

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/jxsl13/amqpx/logging"
)

type SessionPool struct {
	pool              *ConnectionPool
	autoCloseConnPool bool

	transientID int64

	capacity       int
	bufferCapacity int
	confirmable    bool
	sessions       chan *Session

	ctx    context.Context
	cancel context.CancelFunc

	log logging.Logger

	RecoverCallback                     SessionRetryCallback
	PublishRetryCallback                SessionRetryCallback
	GetRetryCallback                    SessionRetryCallback
	ConsumeRetryCallback                SessionRetryCallback
	ConsumeContextRetryCallback         SessionRetryCallback
	ExchangeDeclareRetryCallback        SessionRetryCallback
	ExchangeDeclarePassiveRetryCallback SessionRetryCallback
	ExchangeDeleteRetryCallback         SessionRetryCallback
	QueueDeclareRetryCallback           SessionRetryCallback
	QueueDeclarePassiveRetryCallback    SessionRetryCallback
	QueueDeleteRetryCallback            SessionRetryCallback
	QueueBindRetryCallback              SessionRetryCallback
	QueueUnbindRetryCallback            SessionRetryCallback
	QueuePurgeRetryCallback             SessionRetryCallback
	ExchangeBindRetryCallback           SessionRetryCallback
	ExchangeUnbindRetryCallback         SessionRetryCallback
	QoSRetryCallback                    SessionRetryCallback
	FlowRetryCallback                   SessionRetryCallback
}

func NewSessionPool(pool *ConnectionPool, numSessions int, options ...SessionPoolOption) (*SessionPool, error) {
	if numSessions < 1 {
		return nil, fmt.Errorf("%w: %d", errInvalidPoolSize, numSessions)
	}

	// use sane defaults
	option := sessionPoolOption{
		AutoClosePool:  false, // caller owns the connection pool by default
		Capacity:       numSessions,
		Confirmable:    false,
		BufferCapacity: 10,       // fault tolerance over throughput
		Logger:         pool.log, // derive logger from connection pool
	}

	for _, o := range options {
		o(&option)
	}

	// derive context from connection pool
	return newSessionPoolFromOption(pool, pool.ctx, option)
}

func newSessionPoolFromOption(pool *ConnectionPool, ctx context.Context, option sessionPoolOption) (sp *SessionPool, err error) {
	// decouple from parent context, in case we want to close this context ourselves.
	ctx, cc := context.WithCancelCause(ctx)
	cancel := toCancelFunc(fmt.Errorf("session pool %w", ErrClosed), cc)

	// DO NOT rename this variable to sp
	sessionPool := &SessionPool{
		pool:              pool,
		autoCloseConnPool: option.AutoClosePool,

		bufferCapacity: option.BufferCapacity,
		confirmable:    option.Confirmable,
		capacity:       option.Capacity,
		sessions:       make(chan *Session, option.Capacity),

		ctx:    ctx,
		cancel: cancel,

		log: option.Logger,

		RecoverCallback:                     option.RecoverCallback,
		PublishRetryCallback:                option.PublishRetryCallback,
		GetRetryCallback:                    option.GetRetryCallback,
		ConsumeRetryCallback:                option.ConsumeRetryCallback,
		ConsumeContextRetryCallback:         option.ConsumeContextRetryCallback,
		ExchangeDeclareRetryCallback:        option.ExchangeDeclareRetryCallback,
		ExchangeDeclarePassiveRetryCallback: option.ExchangeDeclarePassiveRetryCallback,
		ExchangeDeleteRetryCallback:         option.ExchangeDeleteRetryCallback,
		QueueDeclareRetryCallback:           option.QueueDeclareRetryCallback,
		QueueDeclarePassiveRetryCallback:    option.QueueDeclarePassiveRetryCallback,
		QueueDeleteRetryCallback:            option.QueueDeleteRetryCallback,
		QueueBindRetryCallback:              option.QueueBindRetryCallback,
		QueueUnbindRetryCallback:            option.QueueUnbindRetryCallback,
		QueuePurgeRetryCallback:             option.QueuePurgeRetryCallback,
		ExchangeBindRetryCallback:           option.ExchangeBindRetryCallback,
		ExchangeUnbindRetryCallback:         option.ExchangeUnbindRetryCallback,
		QoSRetryCallback:                    option.QoSRetryCallback,
		FlowRetryCallback:                   option.FlowRetryCallback,
	}

	sessionPool.debug("initializing pool sessions...")
	defer func() {
		if err != nil {
			sessionPool.error(err, "failed to initialize pool sessions")
		} else {
			sessionPool.info("initialized")
		}
	}()

	err = sessionPool.initCachedSessions()
	if err != nil {
		return nil, err
	}

	return sessionPool, nil
}

func (sp *SessionPool) initCachedSessions() error {
	for i := 0; i < sp.capacity; i++ {
		session, err := sp.initCachedSession(i)
		if err != nil {
			return err
		}
		sp.sessions <- session
	}
	return nil
}

// initCachedSession allows you create a pooled Session.
func (sp *SessionPool) initCachedSession(id int) (*Session, error) {

	// retry until we get a channel
	// or until shutdown
	for {
		conn, err := sp.pool.GetConnection(sp.ctx)
		if err != nil {
			// error is only returned upon shutdown
			return nil, err
		}

		session, err := sp.deriveSession(sp.ctx, conn, id)
		if err != nil {
			sp.pool.ReturnConnection(conn, err)
			continue
		}

		sp.pool.ReturnConnection(conn, nil)
		return session, nil
	}
}

// Size returns the number of available idle sessions in the pool.
func (sp *SessionPool) Size() int {
	return len(sp.sessions)
}

// Capacity returns the size of the session pool which indicate t he number of available cached sessions.
func (sp *SessionPool) Capacity() int {
	return sp.capacity
}

// GetSession gets a pooled session.
// blocks until a session is acquired from the pool.
func (sp *SessionPool) GetSession(ctx context.Context) (s *Session, err error) {
	select {
	case <-sp.catchShutdown():
		return nil, sp.shutdownErr()
	case <-ctx.Done():
		return nil, ctx.Err()
	case session, ok := <-sp.sessions:
		if !ok {
			return nil, fmt.Errorf("failed to get session: %w", ErrClosed)
		}
		defer func() {
			// it's possible for the recovery to fail
			// in that case we MUST return the session back to the pool
			if err != nil {
				sp.ReturnSession(session, err)
			}
		}()

		err := session.Recover(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to get session: %w", err)
		}
		return session, nil
	}
}

// GetTransientSession returns a transient session.
// This method may return an error when the context ha sbeen closed before a session could be obtained.
// A transient session creates a transient connection under the hood.
func (sp *SessionPool) GetTransientSession(ctx context.Context) (s *Session, err error) {
	conn, err := sp.pool.GetTransientConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			sp.pool.ReturnConnection(conn, err)
		}
	}()

	transientID := atomic.AddInt64(&sp.transientID, 1)
	s, err = sp.deriveSession(ctx, conn, int(transientID))
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (sp *SessionPool) deriveSession(ctx context.Context, conn *Connection, id int) (*Session, error) {

	cached := conn.IsCached()

	var name string
	if cached {
		name = fmt.Sprintf("%s-cached-session-%d", conn.Name(), id)
	} else {
		name = fmt.Sprintf("%s-transient-session-%d", conn.Name(), id)
	}

	return NewSession(conn, name,
		SessionWithContext(ctx),
		SessionWithBufferCapacity(sp.bufferCapacity),
		SessionWithCached(cached),
		SessionWithConfirms(sp.confirmable),
		SessionWithAutoCloseConnection(!cached), // only close transient connections
		// reporting/alerting/metrics/etc. callbacks
		SessionWithRecoverCallback(sp.RecoverCallback),
		SessionWithPublishRetryCallback(sp.PublishRetryCallback),
		SessionWithGetRetryCallback(sp.GetRetryCallback),
		SessionWithConsumeRetryCallback(sp.ConsumeRetryCallback),
		SessionWithConsumeContextRetryCallback(sp.ConsumeContextRetryCallback),
		SessionWithExchangeDeclareRetryCallback(sp.ExchangeDeclareRetryCallback),
		SessionWithExchangeDeclarePassiveRetryCallback(sp.ExchangeDeclarePassiveRetryCallback),
		SessionWithExchangeDeleteRetryCallback(sp.ExchangeDeleteRetryCallback),
		SessionWithQueueDeclareRetryCallback(sp.QueueDeclareRetryCallback),
		SessionWithQueueDeclarePassiveRetryCallback(sp.QueueDeclarePassiveRetryCallback),
		SessionWithQueueDeleteRetryCallback(sp.QueueDeleteRetryCallback),
		SessionWithQueueBindRetryCallback(sp.QueueBindRetryCallback),
		SessionWithQueueUnbindRetryCallback(sp.QueueUnbindRetryCallback),
		SessionWithQueuePurgeRetryCallback(sp.QueuePurgeRetryCallback),
		SessionWithExchangeBindRetryCallback(sp.ExchangeBindRetryCallback),
		SessionWithExchangeUnbindRetryCallback(sp.ExchangeUnbindRetryCallback),
		SessionWithQoSRetryCallback(sp.QoSRetryCallback),
		SessionWithFlowRetryCallback(sp.FlowRetryCallback),
	)
}

// ReturnSession returns a Session to the pool.
// If Session is not a cached channel, it is simply closed here.
func (sp *SessionPool) ReturnSession(session *Session, err error) {

	// don't put non-managed sessions back into the channel
	if !session.IsCached() {
		_ = session.Close()
		return
	}

	session.Flag(err)

	// flush confirms channel
	session.Flush()

	// always put the session back into the pool
	// even if the session is still broken
	select {
	case sp.sessions <- session:
	default:
		panic("session buffer full: not supposed to happen")
	}
}

func (sp *SessionPool) catchShutdown() <-chan struct{} {
	return sp.ctx.Done()
}

func (sp *SessionPool) shutdownErr() error {
	return sp.ctx.Err()
}

// Closes the session pool with all of its sessions
func (sp *SessionPool) Close() {

	sp.info("closing session pool...")
	defer sp.info("closed")

	// trigger session cancelation
	// consumers, publishers, etc.
	sp.cancel()

	wg := &sync.WaitGroup{}

	// close all sessions:
	for i := 0; i < sp.capacity; i++ {
		session := <-sp.sessions
		wg.Add(1)
		go func(s *Session) {
			defer wg.Done()
			_ = s.Close()
		}(session)
	}
	wg.Wait()

	if sp.autoCloseConnPool {
		sp.pool.Close()
	}
}

func (sp *SessionPool) info(a ...any) {
	sp.log.WithField("sessionPool", sp.pool.name).Info(a...)
}

func (sp *SessionPool) error(err error, a ...any) {
	sp.log.WithField("sessionPool", sp.pool.name).WithField("error", err.Error()).Error(a...)
}

func (sp *SessionPool) debug(a ...any) {
	sp.log.WithField("sessionPool", sp.pool.name).Debug(a...)
}
