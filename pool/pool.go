package pool

import (
	"context"
	"time"
)

type Pool struct {
	cp *ConnectionPool
	sp *SessionPool
}

func New(connectUrl string, size int, options ...PoolOption) (*Pool, error) {
	if size < 1 {
		panic("max pool size is negative or 0")
	}

	ctx := context.Background()

	// use sane defaults
	option := poolOption{
		cpo: connectionPoolOption{
			Name: defaultAppName(),
			Ctx:  ctx,
			Size: maxi(1, size/2), // at least one connection

			ConnHeartbeatInterval: 15 * time.Second,
			ConnTimeout:           30 * time.Second,
			TLSConfig:             nil,
		},
		spo: sessionPoolOption{
			Size:        size,
			RequireAcks: false,
			BufferSize:  1,   // fault tolerance over throughput
			Ctx:         nil, // initialized further below
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
	option.spo.Ctx = connPool.ctx

	sessPool, err := newSessionPoolFromOption(connPool, option.spo)
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

// ReturnSession returns a Session back to the pool.
// If the session was returned due to an error, erred should be set to true, otherwise
// erred should be set to false.
func (p *Pool) ReturnSession(session *Session, erred bool) (*Session, error) {
	return p.sp.GetSession()
}

func (p *Pool) Context() context.Context {
	// return child context because objects using this pool will rely on the session pool
	return p.sp.ctx
}
