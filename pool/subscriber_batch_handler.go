package pool

import (
	"context"
	"fmt"
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
func NewBatchHandler(queue string, hf BatchHandlerFunc, options ...BatchHandlerOption) *BatchHandler {
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
	mu     sync.Mutex
	closed bool

	queue       string
	handlerFunc BatchHandlerFunc
	consumeOpts ConsumeOptions

	// When <= 0, will be set to 50
	// Number of messages a batch may contain at most
	// before processing is triggered
	maxBatchSize int

	// FlushTimeout is the duration that is waited for the next message from a queue before
	// the batch is closed and passed for processing.
	// This value should be less than 30m (which is the (n)ack timeout of RabbitMQ)
	// when <= 0, will be set to 5s
	flushTimeout time.Duration

	// untouched throughout the object's lifetime
	parentCtx context.Context

	// canceled upon pausing
	pausing *cancelContext

	// canceled upon resuming
	resuming *cancelContext

	// back channel to handler
	// called from consumer
	paused *cancelContext

	// back channel to handler
	// called from consumer
	resumed *cancelContext
}

// reset creates the initial state of the object
// initial state is the transitional state resuming (= startup and resuming after pause)
// the passed context is the parent context of all new contexts that spawn from this
func (h *BatchHandler) start(ctx context.Context) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.parentCtx = ctx

	// reset contextx
	h.pausing.Reset(h.parentCtx)
	h.paused.Reset(h.parentCtx)

	h.resuming.Reset(h.parentCtx)
	h.resumed.Reset(h.parentCtx)

	// cancel last context to indicate the running state
	h.resuming.Cancel() // called last
}

// Pause allows to halt the processing of a queue after the processing has been started by the subscriber.
func (h *BatchHandler) Pause(ctx context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.resuming.Reset(h.parentCtx)
	h.resumed.Reset(h.parentCtx)

	err := h.pausing.CancelWithContext(ctx) // must be called last
	if err != nil {
		return fmt.Errorf("%w: queue: %s: %v", ErrPauseFailed, h.queue, err)
	}

	select {
	case <-h.paused.Done():
		// waid until paused
		return nil
	case <-ctx.Done():
		return fmt.Errorf("%w: queue: %s: %v", ErrPauseFailed, h.queue, ctx.Err())
	}
}

// Resume allows to continue the processing of a queue after it has been paused using Pause
func (h *BatchHandler) Resume(ctx context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.pausing.Reset(h.parentCtx)
	h.paused.Reset(h.parentCtx)

	err := h.resuming.CancelWithContext(h.parentCtx) // must be called last
	if err != nil {
		return fmt.Errorf("%w: queue: %s: %v", ErrResumeFailed, h.queue, err)
	}

	select {
	case <-h.resumed.Done():
		// waid until resumed
		return nil
	case <-ctx.Done():
		return fmt.Errorf("%w: queue: %s: %v", ErrResumeFailed, h.queue, ctx.Err())
	}
}

func (h *BatchHandler) IsActive(ctx context.Context) (active bool, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return false, nil
	}

	select {
	case <-h.resumed.Done():
		return true, nil
	case <-h.paused.Done():
		return false, nil
	case <-ctx.Done():
		return false, fmt.Errorf("failed to check state of handler of queue %q: %w", h.queue, ctx.Err())
	}
}

func (h *BatchHandler) view() batchHandlerView {
	h.mu.Lock()
	defer h.mu.Unlock()

	return batchHandlerView{
		pausing:  h.pausing,  // front channel handler -> consumer
		paused:   h.paused,   // back channel consumer -> handler
		resuming: h.resuming, // front channel handler -> consumer
		resumed:  h.resumed,  // back channel consumer-> handler

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

func (h *BatchHandler) SetHandlerFunc(hf BatchHandlerFunc) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.handlerFunc = hf
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

// close closes all active contexts
// in order to prevent dangling goroutines
func (h *BatchHandler) close() {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.pausing.Cancel()
	h.paused.Cancel()
	h.resuming.Cancel()
	h.resumed.Cancel()
	h.closed = true
}
