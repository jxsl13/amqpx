package pool

import (
	"errors"
	"fmt"
	"strings"
	"sync"
)

var (
	// ErrNotStopped is returned by (Batch)Handler.Resume
	ErrNotStopped = errors.New("cannot resume a not stopped handler")

	// ErrNotRunning is returned by (Batch)Handler.Pause
	ErrNotRunning = errors.New("cannot pause a not running handler")
)

const (
	Stopped  HandlerState = 0
	Starting HandlerState = 1
	Running  HandlerState = 2
)

type HandlerState int

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
		state:       Stopped,
	}
	return h
}

// Handler is a struct that contains all parameters needed in order to register a handler function.
type Handler struct {
	mu sync.Mutex

	queue       string
	handlerFunc HandlerFunc
	consumeOpts ConsumeOptions

	session *Session
	state   HandlerState
	c       chan struct{}
}

type HandlerView struct {
	Queue       string
	HandlerFunc HandlerFunc
	State       HandlerState
	ConsumeOptions
}

func (h *Handler) started(session *Session) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.state != Starting {
		// invalid states: stopped & running
		panic(fmt.Sprintf("invalid handler state for Handler.started (%v)", h.state))
	}

	h.session = session
	h.c = make(chan struct{})
	close(h.c)
	h.state = Running
}

// Pause allows to halt the processing of a queue after the processing has been started by the subscriber.
func (h *Handler) Pause() error {
	h.mu.Lock()
	defer h.mu.Unlock()

	defer func() {
		h.state = Stopped
		h.session = nil
		h.c = make(chan struct{})
	}()

	if h.state != Running {
		if h.consumeOpts.ConsumerTag != "" {
			return fmt.Errorf("%w: consumer: %s queue: %s", ErrNotRunning, h.consumeOpts.ConsumerTag, h.queue)
		} else {
			return fmt.Errorf("%w: queue: %s", ErrNotRunning, h.queue)
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

	if h.state != Stopped {
		if h.consumeOpts.ConsumerTag != "" {
			return fmt.Errorf("%w: consumer: %s queue: %s", ErrNotStopped, h.consumeOpts.ConsumerTag, h.queue)
		} else {
			return fmt.Errorf("%w: queue: %s", ErrNotStopped, h.queue)
		}
	}

	close(h.c)
	return nil
}

func (h *Handler) isRunning() <-chan struct{} {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.c
}

func (h *Handler) State() HandlerState {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.state
}

func (h *Handler) View() HandlerView {
	h.mu.Lock()
	defer h.mu.Unlock()

	return HandlerView{
		Queue:          h.queue,
		HandlerFunc:    h.handlerFunc,
		State:          h.state,
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

func (s HandlerState) MarshalText() (text []byte, err error) {
	var result string
	switch s {
	case Starting:
		result = "starting"
	case Running:
		result = "running"
	case Stopped:
		result = "stopped"
	default:
		return nil, fmt.Errorf("invalid state: %d", int(s))
	}
	return []byte(result), nil
}

func (s *HandlerState) UnmarshalText(text []byte) error {
	switch strings.ToLower(string(text)) {
	case "stopped":
		*s = Stopped
	case "running":
		*s = Running
	case "starting":
		*s = Starting
	default:
		return fmt.Errorf("invalid handler state: %s", string(text))
	}
	return nil
}
