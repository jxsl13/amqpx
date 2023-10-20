package pool

import (
	"context"
)

// handlerView is a a snapshot of the current handler's configuration and runtime state.
// This internal data structure is used in the corresponsing consumer.
type handlerView struct {
	// called in the consumer function & wrapper
	pausingCtx context.Context
	paused     context.CancelFunc

	resumingCtx context.Context
	resumed     context.CancelFunc

	Queue       string
	HandlerFunc HandlerFunc
	ConsumeOptions
}

// HandlerView is a a snapshot of the current handler's configuration and runtime state.
// Interesting for api endpoints that fetch status data.
type HandlerView struct {
	Queue       string
	HandlerFunc HandlerFunc
	ConsumeOptions
}
