package pool

import (
	"context"
)

type sessionOption struct {
	Cached      bool
	Confirmable bool
	BufferSize  int
	Ctx         context.Context
}

type SessionOption func(*sessionOption)

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
// and can be returne dback to the pool without being closed.
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
