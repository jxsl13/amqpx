package pool

import "context"

type sessionPoolOption struct {
	Size        int
	RequireAcks bool // whether messages require acks
	BufferSize  int  // size of the sessio internal confirmation and error buffers.

	Ctx context.Context
}

type SessionPoolOption func(*sessionPoolOption)

// WithContext allows to set a custom connection timeout, that MUST be >= 1 * time.Second
func SessionPoolWithContext(ctx context.Context) SessionPoolOption {
	if ctx == nil {
		panic("nil context passed")
	}
	return func(po *sessionPoolOption) {
		po.Ctx = ctx
	}
}

// SessionPoolWithBufferSize allows to configurethe size of
// the confirmation, error & blocker buffers of all sessions
func SessionPoolWithBufferSize(size int) SessionPoolOption {
	if size < 0 {
		size = 0
	}
	return func(po *sessionPoolOption) {
		po.BufferSize = size
	}
}

// SessionPoolWithAcks requires all messages from sessions to be acked.
func SessionPoolWithRequiredAcks(requireAcks bool) SessionPoolOption {
	return func(po *sessionPoolOption) {
		po.RequireAcks = requireAcks
	}
}
