package pool

import (
	"context"
	"errors"
	"fmt"
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

	ctx := context.Background()
	h := &Handler{
		parentCtx:   ctx,
		pausing:     newCancelContext(ctx),
		paused:      newCancelContext(ctx),
		resuming:    newCancelContext(ctx),
		resumed:     newCancelContext(ctx),
		queue:       queue,
		handlerFunc: hf,
		consumeOpts: copt,
	}

	// initial state is paused
	h.pausing.Cancel()
	h.paused.Cancel()

	return h
}

// Handler is a struct that contains all parameters needed in order to register a handler function
// to the provided queue. Additionally, the handler allows you to pause and resume processing or messages.
type Handler struct {
	mu     sync.Mutex
	closed bool

	queue       string
	handlerFunc HandlerFunc
	consumeOpts ConsumeOptions

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
func (h *Handler) start(ctx context.Context) (err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	defer func() {
		if err != nil {
			h.pausing.Cancel()
			h.paused.Cancel()
			h.resuming.Cancel()
			h.resumed.Cancel()
		}
	}()

	// reset contextx
	err = h.pausing.Reset(h.parentCtx)
	if err != nil {
		return err
	}
	err = h.paused.Reset(h.parentCtx)
	if err != nil {
		return err
	}
	err = h.resuming.Reset(h.parentCtx)
	if err != nil {
		return err
	}
	err = h.resumed.Reset(h.parentCtx)
	if err != nil {
		return err
	}

	// cancel last context to indicate the running state
	h.resuming.Cancel() // called last
	return nil
}

// Pause allows to halt the processing of a queue after the processing has been started by the subscriber.
func (h *Handler) Pause(ctx context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	err := h.resuming.Reset(h.parentCtx)
	if err != nil {
		return fmt.Errorf("%w: queue: %s: %v", ErrPauseFailed, h.queue, err)
	}
	err = h.resumed.Reset(h.parentCtx)
	if err != nil {
		return fmt.Errorf("%w: queue: %s: %v", ErrPauseFailed, h.queue, err)
	}
	err = h.pausing.CancelWithContext(ctx) // must be called last
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
func (h *Handler) Resume(ctx context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	err := h.pausing.Reset(h.parentCtx)
	if err != nil {
		return fmt.Errorf("%w: queue: %s: %v", ErrResumeFailed, h.queue, err)
	}
	err = h.paused.Reset(h.parentCtx)
	if err != nil {
		return fmt.Errorf("%w: queue: %s: %v", ErrResumeFailed, h.queue, err)
	}

	err = h.resuming.CancelWithContext(h.parentCtx) // must be called last
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

func (h *Handler) IsActive(ctx context.Context) (active bool, err error) {
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

func (h *Handler) view() handlerView {
	h.mu.Lock()
	defer h.mu.Unlock()

	return handlerView{
		pausing:        h.pausing,  // front channel handler -> consumer
		paused:         h.paused,   // back channel consumer -> handler
		resuming:       h.resuming, // front channel handler -> consumer
		resumed:        h.resumed,  // back channel consumer-> handler
		Queue:          h.queue,
		HandlerFunc:    h.handlerFunc,
		ConsumeOptions: h.consumeOpts,
	}
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

// close closes all active contexts
// in order to prevent dangling goroutines
func (h *Handler) close() {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.pausing.Cancel()
	h.paused.Cancel()
	h.resuming.Cancel()
	h.resumed.Cancel()
	h.closed = true
}
