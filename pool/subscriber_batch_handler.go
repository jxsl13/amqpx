package pool

import (
	"context"
	"sync"
	"time"
)

const (
	defaultMaxBatchSize = 50
	defaultFlushTimeout = 5 * time.Second
)

// NewHandler creates a new handler which is primarily a combination of your passed
// handler function and the queue name from which the handler fetches messages and processes those.
// Additionally, the handler allows you to pause and resume processing from the provided queue.
func NewBatchHandler(ctx context.Context, queue string, hf BatchHandlerFunc, options ...BatchHandlerOption) *BatchHandler {
	// sane defaults
	h := &BatchHandler{
		maxBatchSize: defaultMaxBatchSize,
		flushTimeout: defaultFlushTimeout,
		queue:        queue,
		handlerFunc:  hf,
		consumeOpts: ConsumeOptions{
			ConsumerTag: "",
			AutoAck:     false,
			Exclusive:   false,
			NoLocal:     false,
			NoWait:      false,
			Args:        nil,
		},
	}

	for _, opt := range options {
		opt(h)
	}

	return h
}

// BatchHandler is a struct that contains all parameter sneeded i order to register a batch handler function.
type BatchHandler struct {
	mu sync.Mutex

	queue       string
	handlerFunc BatchHandlerFunc

	// When <= 0, will be set to 50
	// Number of messages a batch may contain at most
	// before processing is triggered
	maxBatchSize int

	// FlushTimeout is the duration that is waited for the next message from a queue before
	// the batch is closed and passed for processing.
	// This value should be less than 30m (which is the (n)ack timeout of RabbitMQ)
	// when <= 0, will be set to 5s
	flushTimeout time.Duration
	consumeOpts  ConsumeOptions

	// untouched throughout the object's lifetime
	parentCtx context.Context

	// canceled upon pausing
	pausingCtx context.Context
	pause      context.CancelFunc

	// canceled upon resuming
	resumingCtx context.Context
	resume      context.CancelFunc

	// back channel to handler
	pausedCtx context.Context
	// called from consumer
	pausedCancel context.CancelFunc

	// back channel to handler
	resumedCtx context.Context
	// called from consumer
	resumedCancel context.CancelFunc
}

// reset creates the initial state of the object
func (h *BatchHandler) reset() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.pausingCtx, h.pause = context.WithCancel(h.parentCtx)
	h.pausedCtx, h.pausedCancel = context.WithCancel(h.parentCtx)

	h.resumedCtx, h.resumedCancel = context.WithCancel(h.parentCtx)

	h.resumingCtx, h.resume = context.WithCancel(h.parentCtx)
	h.resume() // only the resume channel should be closed initially

	select {
	case <-h.parentCtx.Done():
		return
	case <-h.resumingCtx.Done():
		break
	}
}

func (h *BatchHandler) Pause() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return nil
}

// Resume allows to continue the processing of a queue after it has been paused using Pause
func (h *BatchHandler) Resume(ctx context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	return nil
}

func (h *BatchHandler) view() batchHandlerView {
	h.mu.Lock()
	defer h.mu.Unlock()

	return batchHandlerView{
		pausingCtx:  h.pausingCtx,    // front channel handler -> consumer
		paused:      h.pausedCancel,  // back channel consumer -> handler
		resumingCtx: h.resumedCtx,    // front channel handler -> consumer
		resumed:     h.resumedCancel, // back channel consumer-> handler

		Queue:          h.queue,
		HandlerFunc:    h.handlerFunc,
		MaxBatchSize:   h.maxBatchSize,
		FlushTimeout:   h.flushTimeout,
		ConsumeOptions: h.consumeOpts,
	}
}

func (h *BatchHandler) View() BatchHandlerView {
	h.mu.Lock()
	defer h.mu.Unlock()

	return BatchHandlerView{
		Queue:          h.queue,
		HandlerFunc:    h.handlerFunc,
		MaxBatchSize:   h.maxBatchSize,
		FlushTimeout:   h.flushTimeout,
		ConsumeOptions: h.consumeOpts,
	}
}

func (h *BatchHandler) Queue() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.queue
}

func (h *BatchHandler) SetQueue(queue string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.queue = queue
}

func (h *BatchHandler) MaxBatchSize() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.maxBatchSize
}

func (h *BatchHandler) SetMaxBatchSize(maxBatchSize int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if maxBatchSize <= 0 {
		maxBatchSize = defaultMaxBatchSize
	}
	h.maxBatchSize = maxBatchSize
}

func (h *BatchHandler) FlushTimeout() time.Duration {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.flushTimeout
}

func (h *BatchHandler) SetFlushTimeout(flushTimeout time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if flushTimeout <= 0 {
		flushTimeout = defaultFlushTimeout
	}
	h.flushTimeout = flushTimeout
}

func (h *BatchHandler) ConsumeOptions() ConsumeOptions {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.consumeOpts
}

func (h *BatchHandler) SetConsumeOptions(consumeOpts ConsumeOptions) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.consumeOpts = consumeOpts
}

func (h *BatchHandler) SetHandlerFunc(hf BatchHandlerFunc) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.handlerFunc = hf
}
