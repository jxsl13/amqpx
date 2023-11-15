package pool

import (
	"context"
	"fmt"
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

type stateContext struct {
	mu sync.Mutex

	parentCtx context.Context

	closed bool

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

func newStateContext(ctx context.Context) *stateContext {
	sc := &stateContext{
		parentCtx: ctx,
		pausing:   newCancelContext(ctx),
		resuming:  newCancelContext(ctx),
		paused:    newCancelContext(ctx),
		resumed:   newCancelContext(ctx),
	}

	sc.pausing.Cancel()
	sc.paused.Cancel()
	return sc
}

// reset creates the initial state of the object
// initial state is the transitional state resuming (= startup and resuming after pause)
// the passed context is the parent context of all new contexts that spawn from this.
// After start has been called, all contexts are alive except for the resuming context which is canceled by default.
func (sc *stateContext) Start(ctx context.Context) (err error) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	defer func() {
		if err != nil {
			sc.closeUnguarded()
		}
	}()

	// override upon startup
	sc.parentCtx = ctx
	sc.closed = false

	// reset context
	err = sc.pausing.Reset(sc.parentCtx)
	if err != nil {
		return err
	}
	err = sc.paused.Reset(sc.parentCtx)
	if err != nil {
		return err
	}
	err = sc.resuming.Reset(sc.parentCtx)
	if err != nil {
		return err
	}
	err = sc.resumed.Reset(sc.parentCtx)
	if err != nil {
		return err
	}

	// cancel last context to indicate the running state
	sc.resuming.Cancel() // called last
	return nil
}

func (sc *stateContext) Paused() {
	// explicitly NO mutex lock
	sc.paused.Cancel()
}

func (sc *stateContext) Resumed() {
	// explicitly NO mutex lock
	sc.resumed.Cancel()
}

func (sc *stateContext) Resuming() doner {
	return sc.resuming
}

func (sc *stateContext) Pausing() doner {
	return sc.pausing
}

func (sc *stateContext) Pause(ctx context.Context) error {
	select {
	case <-sc.paused.Done():
		// already paused
		return nil
	default:
		// continue
	}

	err := func() error {
		sc.mu.Lock()
		defer sc.mu.Unlock()
		err := sc.resuming.Reset(sc.parentCtx)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrPauseFailed, err)
		}
		err = sc.resumed.Reset(sc.parentCtx)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrPauseFailed, err)
		}
		err = sc.pausing.CancelWithContext(ctx) // must be called last
		if err != nil {
			return fmt.Errorf("%w: %v", ErrPauseFailed, err)
		}
		return nil
	}()
	if err != nil {
		return err
	}

	select {
	case <-sc.paused.Done():
		// waid until paused
		return nil
	case <-ctx.Done():
		return fmt.Errorf("%w: %v", ErrPauseFailed, ctx.Err())
	}
}

// Resume allows to continue the processing of a queue after it has been paused using Pause
func (sc *stateContext) Resume(ctx context.Context) error {
	select {
	case <-sc.resumed.Done():
		// already resumed
		return nil
	default:
		// continue
	}

	err := func() error {
		sc.mu.Lock()
		defer sc.mu.Unlock()
		err := sc.pausing.Reset(sc.parentCtx)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrResumeFailed, err)
		}
		err = sc.paused.Reset(sc.parentCtx)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrResumeFailed, err)
		}

		err = sc.resuming.CancelWithContext(sc.parentCtx) // must be called last
		if err != nil {
			return fmt.Errorf("%w: %v", ErrResumeFailed, err)
		}
		return nil
	}()
	if err != nil {
		return err
	}

	select {
	case <-sc.resumed.Done():
		// wait until resumed
		return nil
	case <-ctx.Done():
		return fmt.Errorf("%w: %v", ErrResumeFailed, ctx.Err())
	}
}

func (sc *stateContext) IsActive(ctx context.Context) (active bool, err error) {
	closed := func() bool {
		sc.mu.Lock()
		defer sc.mu.Unlock()
		return sc.closed
	}()
	if closed {
		return false, nil
	}

	select {
	case <-sc.resumed.Done():
		return true, nil
	case <-sc.paused.Done():
		return false, nil
	case <-ctx.Done():
		return false, fmt.Errorf("failed to check state: %w", ctx.Err())
	}
}

func (sc *stateContext) AwaitResumed(ctx context.Context) (err error) {
	closed := func() bool {
		sc.mu.Lock()
		defer sc.mu.Unlock()
		return sc.closed
	}()
	if closed {
		return ErrClosed
	}

	select {
	case <-sc.resumed.Done():
		return nil
	case <-ctx.Done():
		return fmt.Errorf("failed to check state: %w", ctx.Err())
	}
}

func (sc *stateContext) AwaitPaused(ctx context.Context) (err error) {
	closed := func() bool {
		sc.mu.Lock()
		defer sc.mu.Unlock()
		return sc.closed
	}()
	if closed {
		return ErrClosed
	}

	select {
	case <-sc.paused.Done():
		return nil
	case <-ctx.Done():
		return fmt.Errorf("failed to check state: %w", ctx.Err())
	}
}

// close closes all active contexts
// in order to prevent dangling goroutines
// When closing you may want to use pause first and then close for the final cleanup
func (sc *stateContext) Close() {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.closeUnguarded()
}

func (sc *stateContext) closeUnguarded() {
	sc.pausing.Cancel()
	sc.paused.Cancel()
	sc.resuming.Cancel()
	sc.resumed.Cancel()
	sc.closed = true
}

type doner interface {
	Done() <-chan struct{}
	Context() context.Context
}
