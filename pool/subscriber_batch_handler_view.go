package pool

import (
	"context"
	"time"
)

// batchHandlerView is a a snapshot of the current handler's configuration and runtime state.
// This internal data structure is used in the corresponsing consumer.
type batchHandlerView struct {
	// called in the consumer function & wrapper
	pausingCtx context.Context
	paused     context.CancelFunc

	resumingCtx context.Context
	resumed     context.CancelFunc

	Queue        string
	HandlerFunc  BatchHandlerFunc
	MaxBatchSize int
	FlushTimeout time.Duration
	ConsumeOptions
}

// BatchHandlerView is a a snapshot of the current handler's configuration and runtime state.
// Interesting for api endpoints that fetch status data.
type BatchHandlerView struct {
	Queue        string
	HandlerFunc  BatchHandlerFunc
	MaxBatchSize int
	FlushTimeout time.Duration
	ConsumeOptions
}
