package pool

import (
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
	switch {
	case errors.As(err, ae):
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

	// every other unknown error is recoverable
	return true
}
