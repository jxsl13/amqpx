package pool

import (
	"time"
)

// BatchHandlerView is a read only snapshot of the current handler's configuration.
type BatchHandlerView struct {
	Queue        string
	HandlerFunc  BatchHandlerFunc
	MaxBatchSize int
	FlushTimeout time.Duration
	ConsumeOptions
}
