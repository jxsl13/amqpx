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
func NewBatchHandler(queue string, hf BatchHandlerFunc, options ...BatchHandlerOption) *BatchHandler {
	if hf == nil {
		panic("handlerFunc must not be nil")
	}

	// sane defaults
	h := &BatchHandler{
		sc:           newStateContext(context.Background()),
		queue:        queue,
		handlerFunc:  hf,
		maxBatchSize: defaultMaxBatchSize,
		flushTimeout: defaultFlushTimeout,
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

// BatchHandler is a struct that contains all parameters needed in order to register a batch handler function.
type BatchHandler struct {
	mu          sync.RWMutex
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

	sc *stateContext
}

// BatchHandlerConfig is a read only snapshot of the current handler's configuration.
type BatchHandlerConfig struct {
	Queue string
	ConsumeOptions

	HandlerFunc  BatchHandlerFunc
	MaxBatchSize int
	FlushTimeout time.Duration
}

func (h *BatchHandler) close() {
	h.sc.Close()
}

// reset creates the initial state of the object
// initial state is the transitional state resuming (= startup and resuming after pause)
// the passed context is the parent context of all new contexts that spawn from this
func (h *BatchHandler) start(ctx context.Context) (opts BatchHandlerConfig, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	opts = h.configUnguarded()
	err = h.sc.Start(ctx)
	return opts, err
}

func (h *BatchHandler) Config() BatchHandlerConfig {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.configUnguarded()
}

func (h *BatchHandler) QueueConfig() QueueConfig {
	h.mu.RLock()
	defer h.mu.RUnlock()

	return QueueConfig{
		Queue:          h.queue,
		ConsumeOptions: h.consumeOpts,
	}
}

func (h *BatchHandler) configUnguarded() BatchHandlerConfig {
	return BatchHandlerConfig{
		Queue:          h.queue,
		HandlerFunc:    h.handlerFunc,
		MaxBatchSize:   h.maxBatchSize,
		FlushTimeout:   h.flushTimeout,
		ConsumeOptions: h.consumeOpts,
	}
}

// Pause allows to halt the processing of a queue after the processing has been started by the subscriber.
func (h *BatchHandler) Pause(ctx context.Context) error {
	return h.sc.Pause(ctx)
}

func (h *BatchHandler) pausing() doner {
	return h.sc.Pausing()
}

func (h *BatchHandler) paused() {
	h.sc.Paused()
}

// Resume allows to continue the processing of a queue after it has been paused using Pause
func (h *BatchHandler) Resume(ctx context.Context) error {
	return h.sc.Resume(ctx)
}

func (h *BatchHandler) resuming() doner {
	return h.sc.Resuming()
}

func (h *BatchHandler) resumed() {
	h.sc.Resumed()
}

func (h *BatchHandler) IsActive(ctx context.Context) (active bool, err error) {
	return h.sc.IsActive(ctx)
}

func (h *BatchHandler) awaitResumed(ctx context.Context) error {
	return h.sc.AwaitResumed(ctx)
}

func (h *BatchHandler) awaitPaused(ctx context.Context) error {
	return h.sc.AwaitPaused(ctx)
}

func (h *BatchHandler) Queue() string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.queue
}

// SetQueue changes the current queue to another queue
// from which the handler consumes messages.
// The actual change is effective after pausing and resuming the handler.
func (h *BatchHandler) SetQueue(queue string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.queue = queue
}

// SetHandlerFunc changes the current handler function  to another
// handler function which processes messages..
// The actual change is effective after pausing and resuming the handler.
func (h *BatchHandler) SetHandlerFunc(hf BatchHandlerFunc) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.handlerFunc = hf
}

func (h *BatchHandler) ConsumeOptions() ConsumeOptions {
	h.mu.RLock()
	defer h.mu.RUnlock()
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
	h.mu.RLock()
	defer h.mu.RUnlock()
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
