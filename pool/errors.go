package pool

import "errors"

var (
	ErrInvalidConnectURL = errors.New("invalid connection url")
	ErrConnectionFailed  = errors.New("connection failed")

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
)
