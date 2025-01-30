package errorutils

import (
	"errors"

	"github.com/rabbitmq/amqp091-go"
)

const (
	notImplemented = 540
)

func Recoverable(err error) bool {
	if err == nil {
		panic("checking nil error for recoverability")
	}

	//  INFO:
	// - ErrClosed, context.Canceled and context.DeadlineExceeded MUST be handled outside of this function
	// bacause network io timeouts are now also considered as context.DeadlineExceeded, we do want them to be recoverable but
	// explicit shutdowns or context cancelations when calling methods, not to be recoverable.
	// - flow control errors are recoverable and are NOT supposed to be handled here

	// invalid usage of the amqp protocol is not recoverable
	// INFO: this should be checked last.
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

	// every other unknown error is recoverable
	return true
}
