package pool

import (
	"github.com/jxsl13/amqpx/logging"
)

type topologerOption struct {
	// TransientOnly makes the topologer only use transient sessions for performing topology actions
	TransientOnly bool
	Logger        logging.Logger
}

type TopologerOption func(*topologerOption)

func TopologerWithLogger(logger logging.Logger) TopologerOption {
	return func(co *topologerOption) {
		co.Logger = logger
	}
}
