package pool

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	defaultMaxBatchSize = 50
	defaultFlushTimeout = 5 * time.Second
)

var (
	// ErrAlreadyPaused is returned when the user tries to pause an already paused handler.
	ErrAlreadyPaused = errors.New("trying to pause an already paused handler")

	// ErrAlreadyRunning is returned when the user tries to resume an already running handler.
	ErrAlreadyRunning = errors.New("trying to resume an already running handler")
)

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
		running:     false,
	}
	return h
}

// Handler is a struct that contains all parameters needed in order to register a handler function.
type Handler struct {
	queue       string
	handlerFunc HandlerFunc
	consumeOpts ConsumeOptions

	mu      sync.Mutex
	session *Session
	running bool
	c       chan struct{}
}

type HandlerView struct {
	Queue       string
	HandlerFunc HandlerFunc
	ConsumeOptions
}

func (h *Handler) started(session *Session) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.running {
		panic("started an already running batch handler")
	}

	h.session = session
	h.c = make(chan struct{})
	close(h.c)
	h.running = true
}

// Pause allows to halt the processing of a queue after the processing has been started by the subscriber.
func (h *Handler) Pause() error {
	h.mu.Lock()
	defer h.mu.Unlock()

	defer func() {
		h.running = false
		h.session = nil
		h.c = make(chan struct{})
	}()

	if !h.running {
		if h.consumeOpts.ConsumerTag != "" {
			return fmt.Errorf("%w: consumer: %s queue: %s", ErrAlreadyPaused, h.consumeOpts.ConsumerTag, h.queue)
		} else {
			return fmt.Errorf("%w: queue: %s", ErrAlreadyPaused, h.queue)
		}
	}

	if h.session == nil {
		panic("handler session is nil")
	}

	err := h.session.Close()
	if err != nil {
		return err
	}
	return nil
}

// Resume allows to continue the processing of a queue after it has been paused using Pause
func (h *Handler) Resume() error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.running {
		if h.consumeOpts.ConsumerTag != "" {
			return fmt.Errorf("%w: consumer: %s queue: %s", ErrAlreadyRunning, h.consumeOpts.ConsumerTag, h.queue)
		} else {
			return fmt.Errorf("%w: queue: %s", ErrAlreadyRunning, h.queue)
		}
	}

	close(h.c)
	h.running = true
	return nil
}

func (h *Handler) isRunning() <-chan struct{} {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.c
}

func (h *Handler) IsRunning() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.running
}

func (h *Handler) View() HandlerView {
	h.mu.Lock()
	defer h.mu.Unlock()

	return HandlerView{
		Queue:          h.queue,
		ConsumeOptions: h.consumeOpts,
		HandlerFunc:    h.handlerFunc,
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
		running: false,
	}

	for _, opt := range options {
		opt(h)
	}

	return h
}

// BatchHandler is a struct that contains all parameter sneeded i order to register a batch handler function.
type BatchHandler struct {
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

	mu      sync.Mutex
	session *Session
	running bool
	c       chan struct{}
}

type BatchHandlerView struct {
	Queue        string
	HandlerFunc  BatchHandlerFunc
	MaxBatchSize int
	FlushTimeout time.Duration
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

func (h *BatchHandler) started(session *Session) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.running {
		panic("started an already running batch handler")
	}

	h.session = session
	h.c = make(chan struct{})
	close(h.c)
	h.running = true
}

func (h *BatchHandler) Pause() error {
	h.mu.Lock()
	defer h.mu.Unlock()

	defer func() {
		h.running = false
		h.session = nil
		h.c = make(chan struct{})
	}()

	if !h.running {
		if h.consumeOpts.ConsumerTag != "" {
			return fmt.Errorf("%w: consumer: %s queue: %s", ErrAlreadyPaused, h.consumeOpts.ConsumerTag, h.queue)
		} else {
			return fmt.Errorf("%w: queue: %s", ErrAlreadyPaused, h.queue)
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

	if h.running {
		if h.consumeOpts.ConsumerTag != "" {
			return fmt.Errorf("%w: consumer: %s queue: %s", ErrAlreadyRunning, h.consumeOpts.ConsumerTag, h.queue)
		} else {
			return fmt.Errorf("%w: queue: %s", ErrAlreadyRunning, h.queue)
		}
	}

	close(h.c)
	h.running = true
	return nil
}

func (h *BatchHandler) isRunning() <-chan struct{} {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.c
}

func (h *BatchHandler) IsRunning() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.running
}
