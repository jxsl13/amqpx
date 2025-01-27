package types

import (
	"errors"
)

const (

	// In order to prevent the broker from requeuing the message to th end of the queue, we need to set this limit in order for at least the
	// first N requeues to be requeued to the front of the queue.
	// https://www.rabbitmq.com/docs/quorum-queues#repeated-requeues
	DefaultQueueDeliveryLimit = 20
)

var (
	ErrInvalidConnectURL = errors.New("invalid connection url")

	// ErrConnectionFailed is just a generic error that is not checked
	// explicitly against in the code.
	ErrConnectionFailed = errors.New("connection failed")

	ErrClosed = errors.New("closed")

	// ErrNotFound is returned by ExchangeDeclarePassive or QueueDeclarePassive in the case that
	// the queue was not found.
	ErrNotFound = errors.New("not found")

	// ErrBlockingFlowControl is returned when the server is under flow control
	// Your HTTP api may return 503 Service Unavailable or 429 Too Many Requests with a Retry-After header (https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Retry-After)
	ErrBlockingFlowControl = errors.New("blocking flow control")

	// errBlockingFlowControlClosed is returned when the flow control channel is closed
	// Specifically interesting when awaiting publish confirms
	// TODO: make public api after a while
	errBlockingFlowControlClosed = errors.New("blocking flow control channel closed")

	// ErrReturned is returned when a message is returned by the server when publishing
	ErrReturned = errors.New("returned")

	// errReturnedClosed
	errReturnedClosed = errors.New("returned channel closed")

	// ErrReject can be used to reject a specific message
	// This is a special error that negatively acknowledges messages and does not reuque them.
	ErrReject = errors.New("message rejected")
)

var (
	// ErrNack is returned in case the broker did not acknowledge a published message
	ErrNack = errors.New("message not acked")

	// returned when a user tries to await confirmations without configuring them for the session
	ErrNoConfirms = errors.New("confirmations are disabled for this session")

	// ErrDeliveryTagMismatch is returne din case we receive a publishing confirmation that
	// contains a delivery tag that doe snot match the one we expect.
	ErrDeliveryTagMismatch = errors.New("delivery tag mismatch")

	ErrDeliveryClosed = errors.New("delivery channel closed")
)
