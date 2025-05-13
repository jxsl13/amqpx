package types

import (
	"context"
	"log/slog"
)

type sessionOption struct {
	Logger         *slog.Logger
	Cached         bool
	Confirmable    bool
	BufferCapacity int
	Ctx            context.Context
	AutoCloseConn  bool

	RecoverCallback                     SessionRetryCallback
	PublishRetryCallback                SessionRetryCallback
	GetRetryCallback                    SessionRetryCallback
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

type SessionOption func(*sessionOption)

// SessionWithLogger allows to set a logger.
// By default no logger is set.
func SessionWithLogger(logger *slog.Logger) SessionOption {
	return func(so *sessionOption) {
		if logger != nil {
			so.Logger = logger
		}
	}
}

// SessionWithContext allows to set a custom session context that might trigger a shutdown
func SessionWithContext(ctx context.Context) SessionOption {
	if ctx == nil {
		panic("nil context passed")
	}
	return func(so *sessionOption) {
		so.Ctx = ctx
	}
}

// SessionWithCached makes a session a cached session
// This is only necessary for the session pool, as cached sessions are part of a pool
// and can be returned back to the pool without being closed.
func SessionWithCached(cached bool) SessionOption {
	return func(so *sessionOption) {
		so.Cached = cached
	}
}

// SessionContext allows enable or explicitly disable message acknowledgements (acks)
func SessionWithConfirms(requiresPublishConfirms bool) SessionOption {
	return func(so *sessionOption) {
		so.Confirmable = requiresPublishConfirms
	}
}

// SessionWithBufferSize allows to customize the size of th einternal channel buffers.
// all buffers/channels are initialized with this size. (e.g. error or confirm channels)
func SessionWithBufferCapacity(capacity int) SessionOption {
	return func(so *sessionOption) {
		so.BufferCapacity = capacity
	}
}

// SessionWithAutoCloseConnection is important for transient sessions
// which, as they allow to create sessions that close their internal connections
// automatically upon closing themselves.
func SessionWithAutoCloseConnection(autoClose bool) SessionOption {
	return func(so *sessionOption) {
		so.AutoCloseConn = autoClose
	}
}

// SessionWithRetryCallback allows to set a custom retry callback for all operations.
// The callback should not interact with anything that may lead to any kind of errors.
// It should preferrably delegate its work to a separate goroutine.
func SessionWithRetryCallback(callback SessionRetryCallback) SessionOption {
	return func(so *sessionOption) {
		so.RecoverCallback = callback
		so.PublishRetryCallback = callback
		so.GetRetryCallback = callback
		so.ConsumeContextRetryCallback = callback
		so.ExchangeDeclareRetryCallback = callback
		so.ExchangeDeclarePassiveRetryCallback = callback
		so.ExchangeDeleteRetryCallback = callback
		so.QueueDeclareRetryCallback = callback
		so.QueueDeclarePassiveRetryCallback = callback
		so.QueueDeleteRetryCallback = callback
		so.QueueBindRetryCallback = callback
		so.QueueUnbindRetryCallback = callback
		so.QueuePurgeRetryCallback = callback
		so.ExchangeBindRetryCallback = callback
		so.ExchangeUnbindRetryCallback = callback
		so.QoSRetryCallback = callback
		so.FlowRetryCallback = callback
	}
}

// SessionWithRecoverCallback allows to set a custom recover callback.
// The callback should not interact with anything that may lead to any kind of errors.
// It should preferrably delegate its work to a separate goroutine.
func SessionWithRecoverCallback(callback SessionRetryCallback) SessionOption {
	return func(so *sessionOption) {
		so.RecoverCallback = callback
	}
}

// SessionWithPublishRetryCallback allows to set a custom publish retry callback.
// The callback should not interact with anything that may lead to any kind of errors.
// It should preferrably delegate its work to a separate goroutine.
func SessionWithPublishRetryCallback(callback SessionRetryCallback) SessionOption {
	return func(so *sessionOption) {
		so.PublishRetryCallback = callback
	}
}

// SessionWithGetRetryCallback allows to set a custom get retry callback.
// The callback should not interact with anything that may lead to any kind of errors.
// It should preferrably delegate its work to a separate goroutine.
func SessionWithGetRetryCallback(callback SessionRetryCallback) SessionOption {
	return func(so *sessionOption) {
		so.GetRetryCallback = callback
	}
}

// SessionWithConsumeContextRetryCallback allows to set a custom consume retry callback.
// The callback should not interact with anything that may lead to any kind of errors.
// It should preferrably delegate its work to a separate goroutine.
func SessionWithConsumeContextRetryCallback(callback SessionRetryCallback) SessionOption {
	return func(so *sessionOption) {
		so.ConsumeContextRetryCallback = callback
	}
}

// SessionWithExchangeDeclareRetryCallback allows to set a custom exchange declare retry callback.
// The callback should not interact with anything that may lead to any kind of errors.
// It should preferrably delegate its work to a separate goroutine.
func SessionWithExchangeDeclareRetryCallback(callback SessionRetryCallback) SessionOption {
	return func(so *sessionOption) {
		so.ExchangeDeclareRetryCallback = callback
	}
}

// SessionWithExchangeDeclarePassiveRetryCallback allows to set a custom exchange declare passive retry callback.
// The callback should not interact with anything that may lead to any kind of errors.
// It should preferrably delegate its work to a separate goroutine.
func SessionWithExchangeDeclarePassiveRetryCallback(callback SessionRetryCallback) SessionOption {
	return func(so *sessionOption) {
		so.ExchangeDeclarePassiveRetryCallback = callback
	}
}

// SessionWithExchangeDeleteRetryCallback allows to set a custom exchange delete retry callback.
// The callback should not interact with anything that may lead to any kind of errors.
// It should preferrably delegate its work to a separate goroutine.
func SessionWithExchangeDeleteRetryCallback(callback SessionRetryCallback) SessionOption {
	return func(so *sessionOption) {
		so.ExchangeDeleteRetryCallback = callback
	}
}

// SessionWithQueueDeclareRetryCallback allows to set a custom queue declare retry callback.
// The callback should not interact with anything that may lead to any kind of errors.
// It should preferrably delegate its work to a separate goroutine.
func SessionWithQueueDeclareRetryCallback(callback SessionRetryCallback) SessionOption {
	return func(so *sessionOption) {
		so.QueueDeclareRetryCallback = callback
	}
}

// SessionWithQueueDeclarePassiveRetryCallback allows to set a custom queue declare passive retry callback.
// The callback should not interact with anything that may lead to any kind of errors.
// It should preferrably delegate its work to a separate goroutine.
func SessionWithQueueDeclarePassiveRetryCallback(callback SessionRetryCallback) SessionOption {
	return func(so *sessionOption) {
		so.QueueDeclarePassiveRetryCallback = callback
	}
}

// SessionWithQueueDeleteRetryCallback allows to set a custom queue delete retry callback.
// The callback should not interact with anything that may lead to any kind of errors.
// It should preferrably delegate its work to a separate goroutine.
func SessionWithQueueDeleteRetryCallback(callback SessionRetryCallback) SessionOption {
	return func(so *sessionOption) {
		so.QueueDeleteRetryCallback = callback
	}
}

// SessionWithQueueBindRetryCallback allows to set a custom queue bind retry callback.
// The callback should not interact with anything that may lead to any kind of errors.
// It should preferrably delegate its work to a separate goroutine.
func SessionWithQueueBindRetryCallback(callback SessionRetryCallback) SessionOption {
	return func(so *sessionOption) {
		so.QueueBindRetryCallback = callback
	}
}

// SessionWithQueueUnbindRetryCallback allows to set a custom queue unbind retry callback.
// The callback should not interact with anything that may lead to any kind of errors.
// It should preferrably delegate its work to a separate goroutine.
func SessionWithQueueUnbindRetryCallback(callback SessionRetryCallback) SessionOption {
	return func(so *sessionOption) {
		so.QueueUnbindRetryCallback = callback
	}
}

// SessionWithQueuePurgeRetryCallback allows to set a custom queue purge retry callback.
// The callback should not interact with anything that may lead to any kind of errors.
// It should preferrably delegate its work to a separate goroutine.
func SessionWithQueuePurgeRetryCallback(callback SessionRetryCallback) SessionOption {
	return func(so *sessionOption) {
		so.QueuePurgeRetryCallback = callback
	}
}

// SessionWithExchangeBindRetryCallback allows to set a custom exchange bind retry callback.
// The callback should not interact with anything that may lead to any kind of errors.
// It should preferrably delegate its work to a separate goroutine.
func SessionWithExchangeBindRetryCallback(callback SessionRetryCallback) SessionOption {
	return func(so *sessionOption) {
		so.ExchangeBindRetryCallback = callback
	}
}

// SessionWithExchangeUnbindRetryCallback allows to set a custom exchange unbind retry callback.
// The callback should not interact with anything that may lead to any kind of errors.
// It should preferrably delegate its work to a separate goroutine.
func SessionWithExchangeUnbindRetryCallback(callback SessionRetryCallback) SessionOption {
	return func(so *sessionOption) {
		so.ExchangeUnbindRetryCallback = callback
	}
}

// SessionWithQoSRetryCallback allows to set a custom qos retry callback.
// The callback should not interact with anything that may lead to any kind of errors.
// It should preferrably delegate its work to a separate goroutine.
func SessionWithQoSRetryCallback(callback SessionRetryCallback) SessionOption {
	return func(so *sessionOption) {
		so.QoSRetryCallback = callback
	}
}

// SessionWithFlowRetryCallback allows to set a custom flow retry callback.
// The callback should not interact with anything that may lead to any kind of errors.
// It should preferrably delegate its work to a separate goroutine.
func SessionWithFlowRetryCallback(callback SessionRetryCallback) SessionOption {
	return func(so *sessionOption) {
		so.FlowRetryCallback = callback
	}
}
