package pool

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jxsl13/amqpx/types"
)

type Pool struct {
	cp *ConnectionPool
	sp *SessionPool
}

func New(ctx context.Context, connectUrl string, numConns, numSessions int, options ...Option) (*Pool, error) {
	if numConns < 1 {
		return nil, fmt.Errorf("%w: %d", errInvalidPoolSize, numConns)
	}

	if numSessions < 1 {
		numSessions = numConns
	}

	logger := slog.New(slog.DiscardHandler)

	// use sane defaults
	option := poolOption{
		cpo: connectionPoolOption{
			Name:     defaultAppName(),
			Ctx:      ctx,
			Capacity: numConns, // at least one connection

			ConnHeartbeatInterval: 15 * time.Second,
			ConnTimeout:           30 * time.Second,
			TLSConfig:             nil,

			Logger: logger,
		},
		spo: sessionPoolOption{
			Capacity:       numSessions,
			Confirmable:    true, // require publish confirmations
			BufferCapacity: 10,

			Logger: logger,
		},
	}

	for _, o := range options {
		o(&option)
	}

	connPool, err := newConnectionPoolFromOption(connectUrl, option.cpo)
	if err != nil {
		return nil, err
	}

	// derive context from connection pool
	sessPool, err := newSessionPoolFromOption(connPool, connPool.ctx, option.spo)
	if err != nil {
		return nil, err
	}

	return &Pool{
		cp: connPool,
		sp: sessPool,
	}, nil
}

func (p *Pool) Close() {
	p.sp.Close()
	p.cp.Close()
}

// ForceGetSession returns a new session from the pool, only returns an error upon shutdown or context cancelation.
// In case that there are no sessions in the session pool, a transient session is created and returned.
// A transient underlying connection is opened and closed when the session is closed.
// You can return (close) the session by returning it back to the pool with [ReturnSession(Session, error)].
func (p *Pool) ForceGetSession(ctx context.Context) (*types.Session, error) {
	return p.sp.ForceGetSession(ctx)
}

// GetSession returns a new session from the pool, only returns an error upon shutdown.
func (p *Pool) GetSession(ctx context.Context) (*types.Session, error) {
	s, err := p.sp.GetSession(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get cached pool session: %w", err)
	}
	return s, nil
}

// GetTransientSession returns a new session which is decoupled from anyshutdown mechanism, thus
// requiring a context for timeout handling.
// The session does also use a transient connection which is closed when the transient session is closed.
func (p *Pool) GetTransientSession(ctx context.Context) (*types.Session, error) {
	s, err := p.sp.GetTransientSession(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get transient pool session: %w", err)
	}
	return s, nil
}

// ReturnSession returns a Session back to the pool.
// If the session was returned due to an error, erred should be set to true, otherwise
// erred should be set to false.
func (p *Pool) ReturnSession(session *types.Session, err error) {
	p.sp.ReturnSession(session, err)
}

func (p *Pool) Context() context.Context {
	// return child context because objects using this pool will rely on the session pool
	return p.sp.ctx
}

func (p *Pool) Name() string {
	return p.cp.Name()
}

// ConnectionPoolCapacity returns the capacity of the connection pool.
func (p *Pool) ConnectionPoolCapacity() int {
	return p.cp.Capacity()
}

// ConnectionPoolSize returns the number of connections in the pool that are idling.
func (p *Pool) ConnectionPoolSize() int {
	return p.cp.Capacity()
}

// SessionPoolCapacity returns the capacity of the session pool.
func (p *Pool) SessionPoolCapacity() int {
	return p.sp.Capacity()
}

// SessionPoolSize returns the number of sessions in the pool that are idling.
func (p *Pool) SessionPoolSize() int {
	return p.sp.Size()
}
