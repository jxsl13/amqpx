package pool

import (
	"context"
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Session is
type Session struct {
	id         int64
	cached     bool
	ackable    bool
	bufferSize int

	channel  *amqp.Channel
	confirms chan amqp.Confirmation
	errors   chan *amqp.Error

	conn *Connection

	mu     sync.Mutex
	ctx    context.Context
	cancel context.CancelFunc
}

// NewSession wraps a connection and a channel in order tointeract with the message broker.
// By default the context of the parent connection is used for cancellation.
func NewSession(conn *Connection, id int64, cached bool, options ...SessionOption) (*Session, error) {
	if conn.IsClosed() {
		return nil, ErrConnectionClosed
	}

	// default values
	option := sessionOption{
		// derive context from connection, as we are derived from the connection
		// so in case the connection is closed, we are closed as well.
		Ctx:        conn.ctx,
		Ackable:    false,
		BufferSize: 100,
	}

	// override default values if options were provided
	for _, o := range options {
		o(&option)
	}

	ctx, cancel := context.WithCancel(option.Ctx)

	session := &Session{
		id:         id,
		cached:     cached,
		ackable:    option.Ackable,
		bufferSize: option.BufferSize,

		channel:  nil, // will be created below
		confirms: nil, // will be created below
		errors:   nil, // will be created below

		conn: conn,

		ctx:    ctx,
		cancel: cancel,
	}

	err := session.Connect()
	if err != nil {
		return nil, fmt.Errorf("failed to create session: %w", err)
	}
	return session, nil
}

// Close closes the session completely.
func (s *Session) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.cancel()
	return s.channel.Close()
}

// Connect tries to create (or re-create) the channel from the Connection it is derived from.
func (s *Session) Connect() (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	defer func() {
		// reset stat in case of an error
		if err != nil {
			s.channel = nil
			s.errors = nil
			s.confirms = nil
		}
	}()

	if s.conn.IsClosed() {
		return ErrConnectionClosed
	}

	channel, err := s.conn.conn.Channel()
	if err != nil {
		return fmt.Errorf("%w: %v", ErrConnectionFailed, err)
	}

	if s.ackable {
		s.confirms = make(chan amqp.Confirmation, s.bufferSize)
		channel.NotifyPublish(s.confirms)

		err = channel.Confirm(false)
		if err != nil {
			return err
		}
	}

	s.errors = make(chan *amqp.Error, s.bufferSize)
	channel.NotifyClose(s.errors)

	return nil
}

// FlushConfirms removes all previous confirmations pending processing.
func (s *Session) FlushConfirms() {

	for {
		// Some weird use case where the Channel is being flooded with confirms after connection disruption
		// It lead to an infinite loop when this method was called.
		select {
		case <-s.confirms:
			// flush confirmations in channel
		case <-s.catchShutdown():
			return
		default:
			return
		}
	}
}

func (s *Session) ID() int64 {
	// read only property after initialization
	return s.id
}

// IsCached returns true in case this session is supposed to be returned to a session pool.
func (s *Session) IsCached() bool {
	// read only property after initialization
	return s.cached
}

func (s *Session) catchShutdown() <-chan struct{} {
	return s.ctx.Done()
}

func (s *Session) isShutdown() bool {
	select {
	case <-s.ctx.Done():
		return true
	default:
		return false
	}
}
