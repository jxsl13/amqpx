package testlogger

import (
	"log/slog"
	"testing"
)

func NewTestLogger(t *testing.T, opts ...*slog.HandlerOptions) *slog.Logger {
	var opt *slog.HandlerOptions = &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}
	if len(opts) > 0 {
		opt = opts[0]
	}

	return slog.New(NewTestHandler(t, opt))
}

func NewTestHandler(t *testing.T, opts *slog.HandlerOptions) slog.Handler {
	return slog.NewTextHandler(
		&writer{t: t},
		opts,
	)
}
