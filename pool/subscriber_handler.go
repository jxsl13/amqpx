package pool

import (
	"context"
	"errors"
	"sync"
)

var (
	// ErrPauseFailed is returned by (Batch)Handler.Pause in case that the passed context is canceled
	ErrPauseFailed = errors.New("failed to pause handler")

	// ErrResumeFailed is returned by (Batch)Handler.Resume in case that the passed context is canceled
	ErrResumeFailed = errors.New("failed to resume handler")
)

// NewHandler creates a new handler which is primarily a combination of your passed
// handler function and the queue name from which the handler fetches messages and processes those.
// Additionally, the handler allows you to pause and resume processing from the provided queue.
func NewHandler(queue string, hf HandlerFunc, option ...ConsumeOptions) *Handler {
	if hf == nil {
		panic("handlerFunc must not be nil")
	}
	copt := ConsumeOptions{
		ConsumerTag: "",
		AutoAck:     false,
		Exclusive:   false,
		NoLocal:     false,
		NoWait:      false,
		Args:        nil,
	}
	if len(option) > 0 {
		copt = option[0]
	}

	h := &Handler{
		queue:       queue,
		handlerFunc: hf,
		consumeOpts: copt,
		sc:          newStateContext(context.Background()),
	}

	return h
}

// Handler is a struct that contains all parameters needed in order to register a handler function
// to the provided queue. Additionally, the handler allows you to pause and resume processing or messages.
type Handler struct {
	mu          sync.Mutex
	queue       string
	handlerFunc HandlerFunc
	consumeOpts ConsumeOptions

	// not guarded by mutex
	sc *stateContext
}

// HandlerConfig is a read only snapshot of the current handler's configuration.
// This internal data structure is used in the corresponsing consumer.
type HandlerConfig struct {
	Queue string
	ConsumeOptions

	HandlerFunc HandlerFunc
}

func (h *Handler) close() {
	h.sc.Close()
}

// reset creates the initial state of the object
// initial state is the transitional state resuming (= startup and resuming after pause)
// the passed context is the parent context of all new contexts that spawn from this.
// After start has been called, all contexts are alive except for the resuming context which is canceled by default.
func (h *Handler) start(ctx context.Context) (opts HandlerConfig, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	opts = h.configUnguarded()
	err = h.sc.Start(ctx)
	return opts, err

}

func (h *Handler) Config() HandlerConfig {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.configUnguarded()
}

func (h *Handler) QueueConfig() QueueConfig {
	h.mu.Lock()
	defer h.mu.Unlock()

	return QueueConfig{
		Queue:          h.queue,
		ConsumeOptions: h.consumeOpts,
	}
}

func (h *Handler) configUnguarded() HandlerConfig {
	return HandlerConfig{
		Queue:          h.queue,
		HandlerFunc:    h.handlerFunc,
		ConsumeOptions: h.consumeOpts,
	}
}

// Pause allows to halt the processing of a queue after the processing has been started by the subscriber.
func (h *Handler) Pause(ctx context.Context) error {
	return h.sc.Pause(ctx)
}

func (h *Handler) pausing() doner {
	return h.sc.Pausing()
}

func (h *Handler) paused() {
	h.sc.Paused()
}

// Resume allows to continue the processing of a queue after it has been paused using Pause
func (h *Handler) Resume(ctx context.Context) error {
	return h.sc.Resume(ctx)
}

func (h *Handler) resuming() doner {
	return h.sc.Resuming()
}

func (h *Handler) resumed() {
	h.sc.Resumed()
}

func (h *Handler) IsActive(ctx context.Context) (active bool, err error) {
	return h.sc.IsActive(ctx)
}

func (h *Handler) awaitResumed(ctx context.Context) error {
	return h.sc.AwaitResumed(ctx)
}

func (h *Handler) awaitPaused(ctx context.Context) error {
	return h.sc.AwaitPaused(ctx)
}

func (h *Handler) Queue() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.queue
}

// SetQueue changes the current queue to another queue
// from which the handler consumes messages.
// The actual change is effective after pausing and resuming the handler.
func (h *Handler) SetQueue(queue string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.queue = queue
}

// SetHandlerFunc changes the current handler function  to another
// handler function which processes messages..
// The actual change is effective after pausing and resuming the handler.
func (h *Handler) SetHandlerFunc(hf HandlerFunc) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.handlerFunc = hf
}

func (h *Handler) ConsumeOptions() ConsumeOptions {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.consumeOpts
}

// SetConsumeOptions changes the current handler function                    to another
// handler function which processes messages..
// The actual change is effective after pausing and resuming the handler.
func (h *Handler) SetConsumeOptions(consumeOpts ConsumeOptions) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.consumeOpts = consumeOpts
}

// QueueConfig is a read only snapshot of the current handler's queue configuration.
// It is the common configuration between the handler and the batch handler.
type QueueConfig struct {
	Queue string
	ConsumeOptions
}
