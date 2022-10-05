package pool

import "context"

type sessionPoolOption struct {
	Size        int
	Confirmable bool // whether published messages require confirmation awaiting
	BufferSize  int  // size of the sessio internal confirmation and error buffers.

	Ctx context.Context
}

type SessionPoolOption func(*sessionPoolOption)

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

// SessionPoolWithConfirms requires all messages from sessions to be acked.
func SessionPoolWithConfirms(requirePublishConfirms bool) SessionPoolOption {
	return func(po *sessionPoolOption) {
		po.Confirmable = requirePublishConfirms
	}
}
