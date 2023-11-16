package pool

import (
	"context"

	"github.com/jxsl13/amqpx/logging"
)

type topologerOption struct {
	TransientOnly bool
	Logger        logging.Logger
	Ctx           context.Context
}

type TopologerOption func(*topologerOption)

func TopologerWithLogger(logger logging.Logger) TopologerOption {
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
