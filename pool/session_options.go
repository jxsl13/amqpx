package pool

import (
	"context"

	"github.com/jxsl13/amqpx/logging"
)

type sessionOption struct {
	Logger        logging.Logger
	Cached        bool
	Confirmable   bool
	BufferSize    int
	Ctx           context.Context
	AutoCloseConn bool
}

type SessionOption func(*sessionOption)

// SessionWithLogger allows to set a logger.
// By default no logger is set.
func SessionWithLogger(logger logging.Logger) SessionOption {
	return func(so *sessionOption) {
		so.Logger = logger
	}
}

// SessionWithContext allows to set a custom session context that might trigger a shutdown
func SessionWithContext(ctx context.Context) SessionOption {
	if ctx == nil {
		panic("nil context passed")
	}
	return func(so *sessionOption) {
		so.Ctx = ctx
	}
}

// SessionWithCached makes a session a cached session
// This is only necessary for the session pool, as cached sessions are part of a pool
// and can be returned back to the pool without being closed.
func SessionWithCached(cached bool) SessionOption {
	return func(so *sessionOption) {
		so.Cached = cached
	}
}

// SessionContext allows enable or explicitly disable message acknowledgements (acks)
func SessionWithConfirms(requiresPublishConfirms bool) SessionOption {
	return func(so *sessionOption) {
		so.Confirmable = requiresPublishConfirms
	}
}

// SessionWithBufferSize allows to customize the size of th einternal channel buffers.
// all buffers/channels are initialized with this size. (e.g. error or confirm channels)
func SessionWithBufferSize(size int) SessionOption {
	return func(so *sessionOption) {
		so.BufferSize = size
	}
}

// SessionWithAutoCloseConnection is important for transient sessions
// which, as they allow to create sessions that close their internal connections
// automatically upon closing themselves.
func SessionWithAutoCloseConnection(autoClose bool) SessionOption {
	return func(so *sessionOption) {
		so.AutoCloseConn = autoClose
	}
}
