package pool

import (
	"context"
	"fmt"
	"time"

	"github.com/jxsl13/amqpx/logging"
	"github.com/rabbitmq/amqp091-go"
)

var (
	QuorumQueue = amqp091.Table{
		"x-queue-type": "quorum",
	}
)

type Pool struct {
	cp *ConnectionPool
	sp *SessionPool
}

func New(connectUrl string, numConns, numSessions int, options ...Option) (*Pool, error) {
	if numConns < 1 {
		return nil, fmt.Errorf("%w: %d", errInvalidPoolSize, numConns)
	}

	if numSessions < 1 {
		numSessions = numConns
	}

	ctx := context.Background()

	logger := logging.NewNoOpLogger()

	// use sane defaults
	option := poolOption{
		cpo: connectionPoolOption{
			Name: defaultAppName(),
			Ctx:  ctx,
			Size: numConns, // at least one connection

			ConnHeartbeatInterval: 15 * time.Second,
			ConnTimeout:           30 * time.Second,
			TLSConfig:             nil,

			Logger: logger,

			SlowClose: false, // needed for goroutine leak tests
		},
		spo: sessionPoolOption{
			Size:        numSessions,
			Confirmable: false, // require publish confirmations
			BufferSize:  1,     // fault tolerance over throughput

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

// GetSession returns a new session from the pool, only returns an error upon shutdown.
func (p *Pool) GetSession() (*Session, error) {
	return p.sp.GetSession()
}

// GetSessionCtx returns a new session from the pool, only returns an error upon shutdown or when the passed context was canceled.
func (p *Pool) GetSessionCtx(ctx context.Context) (*Session, error) {
	if p.sp.ctx == ctx {
		return p.sp.GetSession()
	}
	return p.sp.GetSessionCtx(ctx)
}

// GetTransientSession returns a new session which is decoupled from anyshutdown mechanism, thus
// requiring a context for timeout handling.
// The session does also use a transient connection which is closed when the transient session is closed.
func (p *Pool) GetTransientSession(ctx context.Context) (*Session, error) {
	return p.sp.GetTransientSession(ctx)
}

// ReturnSession returns a Session back to the pool.
// If the session was returned due to an error, erred should be set to true, otherwise
// erred should be set to false.
func (p *Pool) ReturnSession(session *Session, erred bool) {
	p.sp.ReturnSession(session, erred)
}

func (p *Pool) Context() context.Context {
	// return child context because objects using this pool will rely on the session pool
	return p.sp.ctx
}

func (p *Pool) Name() string {
	return p.cp.Name()
}

func (p *Pool) ConnectionPoolSize() int {
	return p.cp.Size()
}

func (p *Pool) SessionPoolSize() int {
	return p.sp.Size()
}
