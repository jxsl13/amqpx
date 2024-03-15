package pool

import (
	"github.com/jxsl13/amqpx/logging"
)

type sessionPoolOption struct {
	Capacity       int
	Confirmable    bool // whether published messages require awaiting confirmations.
	BufferCapacity int  // size of the session internal confirmation and error buffers.

	AutoClosePool bool // whether to close the internal connection pool automatically
	Logger        logging.Logger

	RecoverCallback                     SessionRetryCallback
	PublishRetryCallback                SessionRetryCallback
	GetRetryCallback                    SessionRetryCallback
	ConsumeRetryCallback                SessionRetryCallback
	ConsumeContextRetryCallback         SessionRetryCallback
	ExchangeDeclareRetryCallback        SessionRetryCallback
	ExchangeDeclarePassiveRetryCallback SessionRetryCallback
	ExchangeDeleteRetryCallback         SessionRetryCallback
	QueueDeclareRetryCallback           SessionRetryCallback
	QueueDeclarePassiveRetryCallback    SessionRetryCallback
	QueueDeleteRetryCallback            SessionRetryCallback
	QueueBindRetryCallback              SessionRetryCallback
	QueueUnbindRetryCallback            SessionRetryCallback
	QueuePurgeRetryCallback             SessionRetryCallback
	ExchangeBindRetryCallback           SessionRetryCallback
	ExchangeUnbindRetryCallback         SessionRetryCallback
	QoSRetryCallback                    SessionRetryCallback
	FlowRetryCallback                   SessionRetryCallback
}

type SessionPoolOption func(*sessionPoolOption)

// SessionPoolWithLogger allows to set a custom logger
func SessionPoolWithLogger(logger logging.Logger) SessionPoolOption {
	return func(po *sessionPoolOption) {
		po.Logger = logger
	}
}

// SessionPoolWithBufferCapacity allows to configure the size of
// the confirmation, error & blocker buffers of all sessions
func SessionPoolWithBufferCapacity(capacity int) SessionPoolOption {
	if capacity < 1 {
		capacity = 1 // should be at least 1 in order not to create weiird deadlocks.
	}
	return func(po *sessionPoolOption) {
		po.BufferCapacity = capacity
	}
}

// SessionPoolWithConfirms requires all messages from sessions to be acked.
func SessionPoolWithConfirms(requirePublishConfirms bool) SessionPoolOption {
	return func(po *sessionPoolOption) {
		po.Confirmable = requirePublishConfirms
	}
}

// SessionPoolWithAutoCloseConnectionPool allows to close the internal connection pool automatically.
// This is helpful in case you have a session pool that is the onl yuser of the connection pool.
// You are basically passing ownership of the connection pool to the session pool with this.
func SessionPoolWithAutoCloseConnectionPool(autoClose bool) SessionPoolOption {
	return func(po *sessionPoolOption) {
		po.AutoClosePool = autoClose
	}
}

// SessionPoolWithRetryCallback allows to set a custom retry callback for the session pool.
// This will set the same retry callback for all operations.
func SessionPoolWithRetryCallback(callback SessionRetryCallback) SessionPoolOption {
	return func(po *sessionPoolOption) {
		po.RecoverCallback = callback
		po.PublishRetryCallback = callback
		po.GetRetryCallback = callback
		po.ConsumeRetryCallback = callback
		po.ConsumeContextRetryCallback = callback
		po.ExchangeDeclareRetryCallback = callback
		po.ExchangeDeclarePassiveRetryCallback = callback
		po.ExchangeDeleteRetryCallback = callback
		po.QueueDeclareRetryCallback = callback
		po.QueueDeclarePassiveRetryCallback = callback
		po.QueueDeleteRetryCallback = callback
		po.QueueBindRetryCallback = callback
		po.QueueUnbindRetryCallback = callback
		po.QueuePurgeRetryCallback = callback
		po.ExchangeBindRetryCallback = callback
		po.ExchangeUnbindRetryCallback = callback
		po.QoSRetryCallback = callback
		po.FlowRetryCallback = callback
	}
}

// SessionPoolWithRecoverCallback allows to set a custom recover callback for the session pool.
func SessionPoolWithRecoverCallback(callback SessionRetryCallback) SessionPoolOption {
	return func(po *sessionPoolOption) {
		po.RecoverCallback = callback
	}
}

// SessionPoolWithPublishRetryCallback allows to set a custom publish retry callback for the session pool.
func SessionPoolWithPublishRetryCallback(callback SessionRetryCallback) SessionPoolOption {
	return func(po *sessionPoolOption) {
		po.PublishRetryCallback = callback
	}
}

// SessionPoolWithGetRetryCallback allows to set a custom get retry callback for the session pool.
func SessionPoolWithGetRetryCallback(callback SessionRetryCallback) SessionPoolOption {
	return func(po *sessionPoolOption) {
		po.GetRetryCallback = callback
	}
}

// SessionPoolWithConsumeRetryCallback allows to set a custom consume retry callback for the session pool.
func SessionPoolWithConsumeRetryCallback(callback SessionRetryCallback) SessionPoolOption {
	return func(po *sessionPoolOption) {
		po.ConsumeRetryCallback = callback
	}
}

// SessionPoolWithConsumeContextRetryCallback allows to set a custom consume context retry callback for the session pool.
func SessionPoolWithConsumeContextRetryCallback(callback SessionRetryCallback) SessionPoolOption {
	return func(po *sessionPoolOption) {
		po.ConsumeContextRetryCallback = callback
	}
}

// SessionPoolWithExchangeDeclareRetryCallback allows to set a custom exchange declare retry callback for the session pool.
func SessionPoolWithExchangeDeclareRetryCallback(callback SessionRetryCallback) SessionPoolOption {
	return func(po *sessionPoolOption) {
		po.ExchangeDeclareRetryCallback = callback
	}
}

// SessionPoolWithExchangeDeclarePassiveRetryCallback allows to set a custom exchange declare passive retry callback for the session pool.
func SessionPoolWithExchangeDeclarePassiveRetryCallback(callback SessionRetryCallback) SessionPoolOption {
	return func(po *sessionPoolOption) {
		po.ExchangeDeclarePassiveRetryCallback = callback
	}
}

// SessionPoolWithExchangeDeleteRetryCallback allows to set a custom exchange delete retry callback for the session pool.
func SessionPoolWithExchangeDeleteRetryCallback(callback SessionRetryCallback) SessionPoolOption {
	return func(po *sessionPoolOption) {
		po.ExchangeDeleteRetryCallback = callback
	}
}

// SessionPoolWithQueueDeclareRetryCallback allows to set a custom queue declare retry callback for the session pool.
func SessionPoolWithQueueDeclareRetryCallback(callback SessionRetryCallback) SessionPoolOption {
	return func(po *sessionPoolOption) {
		po.QueueDeclareRetryCallback = callback
	}
}

// SessionPoolWithQueueDeclarePassiveRetryCallback allows to set a custom queue declare passive retry callback for the session pool.
func SessionPoolWithQueueDeclarePassiveRetryCallback(callback SessionRetryCallback) SessionPoolOption {
	return func(po *sessionPoolOption) {
		po.QueueDeclarePassiveRetryCallback = callback
	}
}

// SessionPoolWithQueueDeleteRetryCallback allows to set a custom queue delete retry callback for the session pool.
func SessionPoolWithQueueDeleteRetryCallback(callback SessionRetryCallback) SessionPoolOption {
	return func(po *sessionPoolOption) {
		po.QueueDeleteRetryCallback = callback
	}
}

// SessionPoolWithQueueBindRetryCallback allows to set a custom queue bind retry callback for the session pool.
func SessionPoolWithQueueBindRetryCallback(callback SessionRetryCallback) SessionPoolOption {
	return func(po *sessionPoolOption) {
		po.QueueBindRetryCallback = callback
	}
}

// SessionPoolWithQueueUnbindRetryCallback allows to set a custom queue unbind retry callback for the session pool.
func SessionPoolWithQueueUnbindRetryCallback(callback SessionRetryCallback) SessionPoolOption {
	return func(po *sessionPoolOption) {
		po.QueueUnbindRetryCallback = callback
	}
}

// SessionPoolWithQueuePurgeRetryCallback allows to set a custom queue purge retry callback for the session pool.
func SessionPoolWithQueuePurgeRetryCallback(callback SessionRetryCallback) SessionPoolOption {
	return func(po *sessionPoolOption) {
		po.QueuePurgeRetryCallback = callback
	}
}

// SessionPoolWithExchangeBindRetryCallback allows to set a custom exchange bind retry callback for the session pool.
func SessionPoolWithExchangeBindRetryCallback(callback SessionRetryCallback) SessionPoolOption {
	return func(po *sessionPoolOption) {
		po.ExchangeBindRetryCallback = callback
	}
}

// SessionPoolWithExchangeUnbindRetryCallback allows to set a custom exchange unbind retry callback for the session pool.
func SessionPoolWithExchangeUnbindRetryCallback(callback SessionRetryCallback) SessionPoolOption {
	return func(po *sessionPoolOption) {
		po.ExchangeUnbindRetryCallback = callback
	}
}

// SessionPoolWithQoSRetryCallback allows to set a custom qos retry callback for the session pool.
func SessionPoolWithQoSRetryCallback(callback SessionRetryCallback) SessionPoolOption {
	return func(po *sessionPoolOption) {
		po.QoSRetryCallback = callback
	}
}

// SessionPoolWithFlowRetryCallback allows to set a custom flow retry callback for the session pool.
func SessionPoolWithFlowRetryCallback(callback SessionRetryCallback) SessionPoolOption {
	return func(po *sessionPoolOption) {
		po.FlowRetryCallback = callback
	}
}
