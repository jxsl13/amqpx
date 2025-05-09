package pool

import (
	"context"
	"log/slog"
)

type topologerOption struct {
	TransientOnly bool
	Logger        *slog.Logger
	Ctx           context.Context
}

type TopologerOption func(*topologerOption)

func TopologerWithLogger(logger *slog.Logger) TopologerOption {
	return func(co *topologerOption) {
		co.Logger = logger
	}
}

func TopologerWithTransientSessions(transientOnly bool) TopologerOption {
	return func(co *topologerOption) {
		co.TransientOnly = transientOnly
	}
}

func TopologerWithContext(ctx context.Context) TopologerOption {
	return func(co *topologerOption) {
		co.Ctx = ctx
	}
}
