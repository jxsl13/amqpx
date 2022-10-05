package pool

import (
	"github.com/jxsl13/amqpx/logging"
)

type sessionPoolOption struct {
	Size        int
	Confirmable bool // whether published messages require confirmation awaiting
	BufferSize  int  // size of the sessio internal confirmation and error buffers.

	Logger logging.Logger
}

type SessionPoolOption func(*sessionPoolOption)

// SessionPoolWithLogger allows to set a custom logger
func SessionPoolWithLogger(logger logging.Logger) SessionPoolOption {
	return func(po *sessionPoolOption) {
		po.Logger = logger
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

// SessionPoolWithConfirms requires all messages from sessions to be acked.
func SessionPoolWithConfirms(requirePublishConfirms bool) SessionPoolOption {
	return func(po *sessionPoolOption) {
		po.Confirmable = requirePublishConfirms
	}
}
