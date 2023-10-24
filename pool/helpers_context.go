package pool

import (
	"context"
	"sync"
)

func newCancelContext(parentCtx context.Context) *cancelContext {
	ctx, cancel := context.WithCancel(parentCtx)
	return &cancelContext{
		ctx:    ctx,
		cancel: cancel,
	}
}

type cancelContext struct {
	mu     sync.Mutex
	closed bool
	ctx    context.Context
	cancel context.CancelFunc
}

func (c *cancelContext) CancelWithContext(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cancelWithContext(ctx)
}

func (c *cancelContext) cancelWithContext(ctx context.Context) error {

	if ctx == nil {
		panic("ctx is nil")
	}

	if c.ctx == nil {
		panic("c.ctx is nil")
	}

	if c.closed {
		return nil
	}

	select {
	case <-ctx.Done():
		// unexpectedly aborted cancelation
		return ctx.Err()
	case <-c.ctx.Done():
		// already canceled
		return nil
	default:
		// cancel context
		c.cancel()
		c.closed = true

		// wait for the channel to be closed
		select {
		case <-ctx.Done():
			// unexpectedly aborted cancelation
			return ctx.Err()
		case <-c.ctx.Done():
			// finally canceled
			return nil
		}
	}
}

func (c *cancelContext) Cancel() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.ctx == nil {
		panic("ctx is nil")
	}

	if c.closed {
		return
	}

	select {
	case <-c.ctx.Done():
		// already canceled
		return
	default:
		c.cancel()
		<-c.ctx.Done()
		c.closed = true
	}
}

func (c *cancelContext) Context() context.Context {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.ctx
}

func (c *cancelContext) Done() <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.ctx.Done()
}

// Reset resets the cancel context to be active again
func (c *cancelContext) Reset(parentCtx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.closed {
		// still active, nothing to reset
		return nil
	}
	c.ctx, c.cancel = context.WithCancel(parentCtx)
	c.closed = false
	return nil
}

type done interface {
	Done() <-chan struct{}
	Context() context.Context
}

type cancel interface {
	Cancel()
	CancelWithContext(context.Context) error
}
