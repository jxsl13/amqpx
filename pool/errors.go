package pool

import "errors"

var (
	errInvalidPoolSize = errors.New("invalid pool size")

	ErrPoolInitializationFailed = errors.New("pool initialization failed")

	// ErrPauseFailed is returned by (Batch)Handler.Pause in case that the passed context is canceled
	ErrPauseFailed = errors.New("failed to pause handler")

	// ErrResumeFailed is returned by (Batch)Handler.Resume in case that the passed context is canceled
	ErrResumeFailed = errors.New("failed to resume handler")
)
