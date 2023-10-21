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
func NewHandler(ctx context.Context, queue string, hf HandlerFunc, option ...ConsumeOptions) *Handler {
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
		parentCtx:   ctx,
		queue:       queue,
		handlerFunc: hf,
		consumeOpts: copt,
	}
	// initialize ctx & cancel
	// TODO: do we want to call this here or do we want to call this in the Subscriber.consumer loop before calling handler.view()?
	h.reset()
	return h
}

// Handler is a struct that contains all parameters needed in order to register a handler function
// to the provided queue. Additionally, the handler allows you to pause and resume processing or messages.
type Handler struct {
	mu sync.Mutex

	queue       string
	handlerFunc HandlerFunc
	consumeOpts ConsumeOptions

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
func (h *Handler) reset() {
	h.mu.Lock()
	defer h.mu.Unlock()

	// close old contexts
	closeContext(h.pausingCtx, h.pause)
	closeContext(h.pausedCtx, h.pausedCancel)

	closeContext(h.resumingCtx, h.resume)
	closeContext(h.resumedCtx, h.resumedCancel)

	// create new contexts
	h.pausingCtx, h.pause = context.WithCancel(h.parentCtx)
	h.pausedCtx, h.pausedCancel = context.WithCancel(h.parentCtx)

	h.resumedCtx, h.resumedCancel = context.WithCancel(h.parentCtx)

	h.resumingCtx, h.resume = context.WithCancel(h.parentCtx)

	// cancel last context to indicate the running state
	closeContextWithContext(h.parentCtx, h.resumingCtx, h.resume) // called last
}

// Pause allows to halt the processing of a queue after the processing has been started by the subscriber.
func (h *Handler) Pause(ctx context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.resumingCtx, h.resume = context.WithCancel(h.parentCtx)
	h.resumedCtx, h.resumedCancel = context.WithCancel(h.parentCtx)

	err := closeContextWithContext(ctx, h.pausingCtx, h.pause) // must be called last
	if err != nil {
		return fmt.Errorf("%w: queue: %s: %v", ErrPauseFailed, h.queue, err)
	}

	select {
	case <-h.pausedCtx.Done():
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
	h.pausingCtx, h.pause = context.WithCancel(h.parentCtx)
	h.pausedCtx, h.pausedCancel = context.WithCancel(h.parentCtx)

	err := closeContextWithContext(ctx, h.resumingCtx, h.resume) // must be called last
	if err != nil {
		return fmt.Errorf("%w: queue: %s: %v", ErrResumeFailed, h.queue, err)
	}

	select {
	case <-h.resumedCtx.Done():
		// waid until resumed
		return nil
	case <-ctx.Done():
		return fmt.Errorf("%w: queue: %s: %v", ErrResumeFailed, h.queue, ctx.Err())
	}
}

func (h *Handler) view() handlerView {
	h.mu.Lock()
	defer h.mu.Unlock()

	return handlerView{
		pausingCtx:     h.pausingCtx,    // front channel handler -> consumer
		paused:         h.pausedCancel,  // back channel consumer -> handler
		resumingCtx:    h.resumingCtx,   // front channel handler -> consumer
		resumed:        h.resumedCancel, // back channel consumer-> handler
		Queue:          h.queue,
		HandlerFunc:    h.handlerFunc,
		ConsumeOptions: h.consumeOpts,
	}
}

func (h *Handler) View() HandlerView {
	h.mu.Lock()
	defer h.mu.Unlock()

	return HandlerView{
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

func (h *Handler) SetQueue(queue string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.queue = queue
}

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

func (h *Handler) SetConsumeOptions(consumeOpts ConsumeOptions) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.consumeOpts = consumeOpts
}

// TODO: clean close
func (h *Handler) close() {
	h.mu.Lock()
	defer h.mu.Unlock()

	closeContext(h.pausingCtx, h.pause)
	closeContext(h.pausedCtx, h.pausedCancel)

	closeContext(h.resumingCtx, h.resume)
	closeContext(h.resumedCtx, h.resumedCancel)
}

func closeContext(ctx context.Context, cancel context.CancelFunc) {

	if ctx == nil {
		return
	}

	select {
	case <-ctx.Done():
		// already canceled
		return
	default:
		cancel()
		<-ctx.Done()
	}
}

func closeContextWithContext(ctx, canceledContext context.Context, cancel context.CancelFunc) error {
	if canceledContext == nil {
		return errors.New("canceledContext is nil")
	}
	select {
	case <-ctx.Done():
		// unexpectedly aborted cancelation
		return ctx.Err()
	case <-canceledContext.Done():
		// already canceled
		return nil
	default:
		// cancel context
		cancel()

		// wait for the channel to be closed
		select {
		case <-ctx.Done():
			// unexpectedly aborted cancelation
			return ctx.Err()
		case <-canceledContext.Done():
			// finally canceled
			return nil
		}
	}
}
