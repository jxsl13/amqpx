package pool

import (
	"context"
)

type sessionOption struct {
	Ctx        context.Context
	Ackable    bool
	BufferSize int
}

type SessionOption func(*sessionOption)

// SessionContext allows to set a custom session context that might trigger a shutdown
func SessionContext(ctx context.Context) SessionOption {
	if ctx == nil {
		panic("nil context passed")
	}
	return func(so *sessionOption) {
		so.Ctx = ctx
	}
}

// SessionContext allows enable or explicitly disable message acknowledgements (acks)
func SessionWithAckableMessages(ackable bool) SessionOption {
	return func(so *sessionOption) {
		so.Ackable = ackable
	}
}

// SessionWithBufferSize allows to customize the size of th einternal channel buffers.
// all buffers/channels are initialized with this size. (e.g. error or confirm channels)
func SessionWithBufferSize(ackable bool) SessionOption {
	return func(so *sessionOption) {
		so.Ackable = ackable
	}
}
