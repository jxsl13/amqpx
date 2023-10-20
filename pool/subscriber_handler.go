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

// Pause allows to halt the processing of a queue after the processing has been started by the subscriber.
func (h *Handler) Pause(ctx context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.resumingCtx, h.resume = context.WithCancel(h.parentCtx)
	h.resumedCtx, h.resumedCancel = context.WithCancel(h.parentCtx)
	h.pause() // must be called last

	select {
	case <-ctx.Done():
		return fmt.Errorf("%w: queue: %s: %v", ErrPauseFailed, h.queue, ctx.Err())
	case <-h.pausedCtx.Done():
		return nil
	}
}

// Resume allows to continue the processing of a queue after it has been paused using Pause
func (h *Handler) Resume(ctx context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.pausingCtx, h.pause = context.WithCancel(h.parentCtx)
	h.pausedCtx, h.pausedCancel = context.WithCancel(h.parentCtx)

	h.resume() // must be calle dlast

	select {
	case <-ctx.Done():
		return fmt.Errorf("%w: queue: %s: %v", ErrResumeFailed, h.queue, ctx.Err())
	case <-h.resumedCtx.Done():
		return nil
	}
}

func (h *Handler) view() handlerView {
	h.mu.Lock()
	defer h.mu.Unlock()

	return handlerView{
		pausingCtx:     h.pausingCtx,    // front channel handler -> consumer
		paused:         h.pausedCancel,  // back channel consumer -> handler
		resumingCtx:    h.resumedCtx,    // front channel handler -> consumer
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

	closeCtx(h.pausingCtx, h.pause)
	closeCtx(h.pausedCtx, h.pausedCancel)

	closeCtx(h.resumingCtx, h.resume)
	closeCtx(h.resumedCtx, h.resumedCancel)
}

func closeCtx(ctx context.Context, cancel context.CancelFunc) {
	select {
	case <-ctx.Done():
		// already canceled
		return
	default:
		cancel()
	}
}
