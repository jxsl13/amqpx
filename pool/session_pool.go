package pool

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/jxsl13/amqpx/internal/contextutils"
	"github.com/jxsl13/amqpx/types"
)

type SessionPool struct {
	pool              *ConnectionPool
	autoCloseConnPool bool

	transientID uint64

	capacity       int
	bufferCapacity int
	confirmable    bool
	sessions       chan *types.Session

	ctx    context.Context
	cancel context.CancelFunc

	log *slog.Logger

	RecoverCallback                     types.SessionRetryCallback
	PublishRetryCallback                types.SessionRetryCallback
	GetRetryCallback                    types.SessionRetryCallback
	ConsumeContextRetryCallback         types.SessionRetryCallback
	ExchangeDeclareRetryCallback        types.SessionRetryCallback
	ExchangeDeclarePassiveRetryCallback types.SessionRetryCallback
	ExchangeDeleteRetryCallback         types.SessionRetryCallback
	QueueDeclareRetryCallback           types.SessionRetryCallback
	QueueDeclarePassiveRetryCallback    types.SessionRetryCallback
	QueueDeleteRetryCallback            types.SessionRetryCallback
	QueueBindRetryCallback              types.SessionRetryCallback
	QueueUnbindRetryCallback            types.SessionRetryCallback
	QueuePurgeRetryCallback             types.SessionRetryCallback
	ExchangeBindRetryCallback           types.SessionRetryCallback
	ExchangeUnbindRetryCallback         types.SessionRetryCallback
	QoSRetryCallback                    types.SessionRetryCallback
	FlowRetryCallback                   types.SessionRetryCallback
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
	cancel := contextutils.ToCancelFunc(fmt.Errorf("session pool %w", types.ErrClosed), cc)

	// DO NOT rename this variable to sp
	sessionPool := &SessionPool{
		pool:              pool,
		autoCloseConnPool: option.AutoClosePool,

		bufferCapacity: option.BufferCapacity,
		confirmable:    option.Confirmable,
		capacity:       option.Capacity,
		sessions:       make(chan *types.Session, option.Capacity),

		ctx:    ctx,
		cancel: cancel,

		log: option.Logger,

		RecoverCallback:                     option.RecoverCallback,
		PublishRetryCallback:                option.PublishRetryCallback,
		GetRetryCallback:                    option.GetRetryCallback,
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

	log := sessionPool.slog()
	log.Debug("initializing pool sessions...")
	defer func() {
		if err != nil {
			log.Error(fmt.Sprintf("failed to initialize pool sessions: %s", err.Error()))
		} else {
			log.Info("initialized session pool")
		}
	}()

	err = sessionPool.initCachedSessions()
	if err != nil {
		return nil, err
	}

	return sessionPool, nil
}

func (sp *SessionPool) initCachedSessions() error {
	for i := uint64(0); i < uint64(sp.capacity); i++ {
		session, err := sp.initCachedSession(i)
		if err != nil {
			return err
		}
		sp.sessions <- session
	}
	return nil
}

// initCachedSession allows you create a pooled Session.
func (sp *SessionPool) initCachedSession(id uint64) (_ *types.Session, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("failed to initialize cached session %d: %w", id, err)
		}
	}()
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

func (sp *SessionPool) ForceGetSession(ctx context.Context) (s *types.Session, err error) {
	select {
	case <-sp.catchShutdown():
		return nil, fmt.Errorf("failed to get session pool session: %w", sp.shutdownErr())
	case <-ctx.Done():
		return nil, fmt.Errorf("failed to get session pool session: %w", ctx.Err())
	case session, ok := <-sp.sessions:
		if !ok {
			return nil, fmt.Errorf("failed to get session pool session: %w", types.ErrClosed)
		}
		defer func() {
			// it's possible for the recovery to fail
			// in that case we MUST return the session back to the pool
			if err != nil {
				sp.ReturnSession(session, err)
			}
		}()
		sp.slog().Debug(fmt.Sprintf("using session from session pool: %s", session.Name()))
		err := session.Recover(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to recover session pool session: %w", err)
		}
		return session, nil
	default:
		sp.slog().Debug("session pool is empty, creating transient session")
		return sp.GetTransientSession(ctx)
	}
}

// GetSession gets a pooled session.
// blocks until a session is acquired from the pool.
func (sp *SessionPool) GetSession(ctx context.Context) (s *types.Session, err error) {
	select {
	case <-sp.catchShutdown():
		return nil, fmt.Errorf("failed to get session pool session: %w", sp.shutdownErr())
	case <-ctx.Done():
		return nil, fmt.Errorf("failed to get session pool session: %w", ctx.Err())
	case session, ok := <-sp.sessions:
		if !ok {
			return nil, fmt.Errorf("failed to get session pool session: %w", types.ErrClosed)
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
			return nil, fmt.Errorf("failed to recover session pool session: %w", err)
		}
		return session, nil
	}
}

// GetTransientSession returns a transient session.
// This method may return an error when the context has been closed before a session could be obtained.
// A transient session creates a transient connection under the hood.
func (sp *SessionPool) GetTransientSession(ctx context.Context) (s *types.Session, err error) {
	conn, err := sp.pool.GetTransientConnection(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get transient session pool connection: %w", err)
	}
	defer func() {
		if err != nil {
			sp.pool.ReturnConnection(conn, err)
		}
	}()

	transientID := atomic.AddUint64(&sp.transientID, 1)
	s, err = sp.deriveSession(ctx, conn, transientID)
	if err != nil {
		return nil, fmt.Errorf("failed to create transient session pool session: %w", err)
	}
	return s, nil
}

func (sp *SessionPool) deriveSession(ctx context.Context, conn *types.Connection, id uint64) (*types.Session, error) {

	cached := conn.IsCached()

	var name string
	if cached {
		name = fmt.Sprintf("%s-cached-session-%d", conn.Name(), id)
	} else {
		name = fmt.Sprintf("%s-transient-session-%d", conn.Name(), id)
	}

	return types.NewSession(conn, name,
		types.SessionWithContext(ctx),
		types.SessionWithBufferCapacity(sp.bufferCapacity),
		types.SessionWithCached(cached),
		types.SessionWithConfirms(sp.confirmable),
		types.SessionWithAutoCloseConnection(!cached), // only close transient connections
		// reporting/alerting/metrics/etc. callbacks
		types.SessionWithRecoverCallback(sp.RecoverCallback),
		types.SessionWithPublishRetryCallback(sp.PublishRetryCallback),
		types.SessionWithGetRetryCallback(sp.GetRetryCallback),
		types.SessionWithConsumeContextRetryCallback(sp.ConsumeContextRetryCallback),
		types.SessionWithExchangeDeclareRetryCallback(sp.ExchangeDeclareRetryCallback),
		types.SessionWithExchangeDeclarePassiveRetryCallback(sp.ExchangeDeclarePassiveRetryCallback),
		types.SessionWithExchangeDeleteRetryCallback(sp.ExchangeDeleteRetryCallback),
		types.SessionWithQueueDeclareRetryCallback(sp.QueueDeclareRetryCallback),
		types.SessionWithQueueDeclarePassiveRetryCallback(sp.QueueDeclarePassiveRetryCallback),
		types.SessionWithQueueDeleteRetryCallback(sp.QueueDeleteRetryCallback),
		types.SessionWithQueueBindRetryCallback(sp.QueueBindRetryCallback),
		types.SessionWithQueueUnbindRetryCallback(sp.QueueUnbindRetryCallback),
		types.SessionWithQueuePurgeRetryCallback(sp.QueuePurgeRetryCallback),
		types.SessionWithExchangeBindRetryCallback(sp.ExchangeBindRetryCallback),
		types.SessionWithExchangeUnbindRetryCallback(sp.ExchangeUnbindRetryCallback),
		types.SessionWithQoSRetryCallback(sp.QoSRetryCallback),
		types.SessionWithFlowRetryCallback(sp.FlowRetryCallback),
	)
}

// ReturnSession returns a Session to the pool.
// If Session is not a cached channel, it is simply closed here.
func (sp *SessionPool) ReturnSession(session *types.Session, err error) {

	log := sp.slog()
	// don't put unmanaged sessions back into the pool channel
	if !session.IsCached() {
		cerr := session.Close()
		if cerr != nil {
			log.Warn(fmt.Sprintf("failed to close transient session %s: %s", session.Name(), cerr.Error()))
		}
		return
	}

	session.Flag(err)

	session.FlushConfirms()
	session.FlushReturned()

	// always put the session back into the pool
	// even if the session is still broken
	select {
	case sp.sessions <- session:
		log.Debug(fmt.Sprintf("returned session %s to pool", session.Name()))
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

	log := sp.slog()
	log.Debug("closing session pool...")
	defer log.Info("closed session pool")

	// trigger session cancelation
	// consumers, publishers, etc.
	sp.cancel()

	wg := &sync.WaitGroup{}

	// close all sessions:
	for i := 0; i < sp.capacity; i++ {
		session := <-sp.sessions
		wg.Add(1)
		go func(s *types.Session) {
			defer wg.Done()
			cerr := s.Close()
			if cerr != nil {
				log.Error(fmt.Sprintf("failed to close session %s", s.Name()))
			} else {
				log.Debug(fmt.Sprintf("closed session %s", s.Name()))
			}
		}(session)
	}
	wg.Wait()

	if sp.autoCloseConnPool {
		sp.pool.Close()
	}
}

func (sp *SessionPool) slog() *slog.Logger {
	return sp.log.With(
		slog.String("session_pool", sp.pool.name),
	)
}
