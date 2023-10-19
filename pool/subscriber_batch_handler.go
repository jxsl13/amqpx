package pool

import (
	"fmt"
	"sync"
	"time"
)

const (
	defaultMaxBatchSize = 50
	defaultFlushTimeout = 5 * time.Second
)

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
		state: Stopped,
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

	session *Session
	state   HandlerState
	c       chan struct{}
}

type BatchHandlerView struct {
	Queue        string
	HandlerFunc  BatchHandlerFunc
	MaxBatchSize int
	FlushTimeout time.Duration
	State        HandlerState
	ConsumeOptions
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
		State:          h.state,
	}
}

func (h *BatchHandler) started(session *Session) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.state != Starting {
		// invalid states: stopped & running
		panic(fmt.Sprintf("invalid handler state for BatchHandler.started (%v)", h.state))
	}

	h.session = session
	h.c = make(chan struct{})
	close(h.c)
	h.state = Running
}

func (h *BatchHandler) Pause() error {
	h.mu.Lock()
	defer h.mu.Unlock()

	defer func() {
		h.state = Stopped
		h.session = nil
		h.c = make(chan struct{})
	}()

	// Starting & Stopped not allowed
	if h.state != Running {
		if h.consumeOpts.ConsumerTag != "" {
			return fmt.Errorf("%w: consumer: %s queue: %s", ErrNotRunning, h.consumeOpts.ConsumerTag, h.queue)
		} else {
			return fmt.Errorf("%w: queue: %s", ErrNotRunning, h.queue)
		}
	}

	err := h.session.Close()
	if err != nil {
		return err
	}
	return nil
}

func (h *BatchHandler) Resume() error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.state != Stopped {
		if h.consumeOpts.ConsumerTag != "" {
			return fmt.Errorf("%w: consumer: %s queue: %s", ErrNotStopped, h.consumeOpts.ConsumerTag, h.queue)
		} else {
			return fmt.Errorf("%w: queue: %s", ErrNotStopped, h.queue)
		}
	}

	close(h.c)
	h.state = Starting
	return nil
}

func (h *BatchHandler) isRunning() <-chan struct{} {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.c
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

func (h *BatchHandler) State() HandlerState {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.state
}
