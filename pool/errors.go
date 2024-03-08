package pool

import (
	"context"
	"errors"

	"github.com/rabbitmq/amqp091-go"
)

var (
	ErrInvalidConnectURL = errors.New("invalid connection url")

	// ErrConnectionFailed is just a generic error that is not checked
	// explicitly against in the code.
	ErrConnectionFailed = errors.New("connection failed")

	errInvalidPoolSize          = errors.New("invalid pool size")
	ErrPoolInitializationFailed = errors.New("pool initialization failed")
	ErrClosed                   = errors.New("closed")

	// ErrNotFound is returned by ExchangeDeclarePassive or QueueDeclarePassive in the case that
	// the queue was not found.
	ErrNotFound = errors.New("not found")

	// ErrFlowControl is returned when the server is under flow control
	// Your HTTP api may return 503 Service Unavailable or 429 Too Many Requests with a Retry-After header (https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Retry-After)
	ErrFlowControl = errors.New("flow control")

	// errFlowControlClosed is returned when the flow control channel is closed
	// Specifically interesting when awaiting publish confirms
	// TODO: make public api after a while
	errFlowControlClosed = errors.New("flow control channel closed")

	// ErrReturned is returned when a message is returned by the server when publishing
	ErrReturned = errors.New("returned")

	// errReturnedClosed
	errReturnedClosed = errors.New("returned channel closed")

	// ErrReject can be used to reject a specific message
	// This is a special error that negatively acknowledges messages and does not reuque them.
	ErrReject = errors.New("message rejected")

	// ErrRejectSingle can be used to reject a specific message
	// This is a special error that negatively acknowledges messages and does not reuque them
	ErrRejectSingle = errors.New("single message rejected")
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

func recoverable(err error) bool {
	if err == nil {
		panic("checking nil error for recoverability")
	}

	// invalid usage of the amqp protocol is not recoverable
	ae := &amqp091.Error{}
	if errors.As(err, &ae) {
		switch ae.Code {
		case notImplemented:
			return false
		default:
			// recoverability according to amqp091 is when
			// the result can be changing by changing use rinput.

			// recoverability according to this library is
			// changing the result by reconnecting

			// not recoverable by changing user input
			// because of potential connection loss
			return !ae.Recover
		}
	}

	if errors.Is(err, context.Canceled) {
		return false
	}

	// TODO: errors.Is(err, context.DeadlineExceeded) also needed?

	// every other unknown error is recoverable
	return true
}
