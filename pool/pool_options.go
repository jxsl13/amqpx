package pool

import (
	"crypto/tls"
	"time"

	"github.com/jxsl13/amqpx/logging"
	"github.com/jxsl13/amqpx/types"
)

type poolOption struct {
	cpo connectionPoolOption
	spo sessionPoolOption
}

type Option func(*poolOption)

// WithName gives all of your pooled connections a prefix name
func WithName(name string) Option {
	if len(name) == 0 {
		name = defaultAppName()
	}
	return func(po *poolOption) {
		ConnectionPoolWithName(name)(&po.cpo)
	}
}

// WithNameSuffix adds a suffix to the connection pool name
func WithNameSuffix(suffix string) Option {
	return func(po *poolOption) {
		ConnectionPoolWithNameSuffix(suffix)(&po.cpo)
	}
}

// WithNamePrefix adds a prefix to the connection pool name
func WithNamePrefix(prefix string) Option {
	return func(po *poolOption) {
		ConnectionPoolWithNamePrefix(prefix)(&po.cpo)
	}
}

// WithLogger allows to set a custom logger for the connection AND session pool
func WithLogger(logger logging.Logger) Option {
	return func(po *poolOption) {
		ConnectionPoolWithLogger(logger)(&po.cpo)
		SessionPoolWithLogger(logger)(&po.spo)
	}
}

// WithHeartbeatInterval allows to set a custom heartbeat interval, that MUST be >= 1 * time.Second
func WithHeartbeatInterval(interval time.Duration) Option {
	return func(po *poolOption) {
		ConnectionPoolWithHeartbeatInterval(interval)(&po.cpo)
	}
}

// WithConnectionTimeout allows to set a custom connection timeout, that MUST be >= 1 * time.Second
func WithConnectionTimeout(timeout time.Duration) Option {
	return func(po *poolOption) {
		ConnectionPoolWithConnectionTimeout(timeout)(&po.cpo)
	}
}

// WithTLS allows to configure tls connectivity.
func WithTLS(config *tls.Config) Option {
	return func(po *poolOption) {
		ConnectionPoolWithTLS(config)(&po.cpo)
	}
}

// WithBufferCapacity allows to configurethe size of
// the confirmation, error & blocker buffers of all sessions
func WithBufferCapacity(size int) Option {
	return func(po *poolOption) {
		SessionPoolWithBufferCapacity(size)(&po.spo)
	}
}

// WithConfirms requires all messages from sessions to be acked.
func WithConfirms(requirePublishConfirms bool) Option {
	return func(po *poolOption) {
		SessionPoolWithConfirms(requirePublishConfirms)(&po.spo)
	}
}

// WithConnectionRecoverCallback allows to set a custom connection recovery callback
func WithConnectionRecoverCallback(callback types.ConnectionRecoverCallback) Option {
	return func(po *poolOption) {
		ConnectionPoolWithRecoverCallback(callback)(&po.cpo)
	}
}

// WithSessionRecoverCallback allows to set a custom session recovery callback
func WithSessionRecoverCallback(callback types.SessionRetryCallback) Option {
	return func(po *poolOption) {
		SessionPoolWithRetryCallback(callback)(&po.spo)
	}
}

// WithSessionRetryCallback allows to set a custom retry callback for the session pool.
// This will set the same retry callback for all operations.
func WithSessionRetryCallback(callback types.SessionRetryCallback) Option {
	return func(po *poolOption) {
		SessionPoolWithRetryCallback(callback)(&po.spo)
	}
}

// WithSessionPublishRetryCallback allows to set a custom retry callback for the session pool.
func WithSessionPublishRetryCallback(callback types.SessionRetryCallback) Option {
	return func(po *poolOption) {
		SessionPoolWithPublishRetryCallback(callback)(&po.spo)
	}
}

// WithSessionGetRetryCallback allows to set a custom retry callback for the session pool.
func WithSessionGetRetryCallback(callback types.SessionRetryCallback) Option {
	return func(po *poolOption) {
		SessionPoolWithGetRetryCallback(callback)(&po.spo)
	}
}

// WithSessionConsumeContextRetryCallback allows to set a custom retry callback for the session pool.
func WithSessionConsumeContextRetryCallback(callback types.SessionRetryCallback) Option {
	return func(po *poolOption) {
		SessionPoolWithConsumeContextRetryCallback(callback)(&po.spo)
	}
}

// WithSessionExchangeDeclareRetryCallback allows to set a custom retry callback for the session pool.
func WithSessionExchangeDeclareRetryCallback(callback types.SessionRetryCallback) Option {
	return func(po *poolOption) {
		SessionPoolWithExchangeDeclareRetryCallback(callback)(&po.spo)
	}
}

// WithSessionExchangeDeclarePassiveRetryCallback allows to set a custom retry callback for the session pool.
func WithSessionExchangeDeclarePassiveRetryCallback(callback types.SessionRetryCallback) Option {
	return func(po *poolOption) {
		SessionPoolWithExchangeDeclarePassiveRetryCallback(callback)(&po.spo)
	}
}

// WithSessionExchangeDeleteRetryCallback allows to set a custom retry callback for the session pool.
func WithSessionExchangeDeleteRetryCallback(callback types.SessionRetryCallback) Option {
	return func(po *poolOption) {
		SessionPoolWithExchangeDeleteRetryCallback(callback)(&po.spo)
	}
}

// WithSessionQueueDeclareRetryCallback allows to set a custom retry callback for the session pool.
func WithSessionQueueDeclareRetryCallback(callback types.SessionRetryCallback) Option {
	return func(po *poolOption) {
		SessionPoolWithQueueDeclareRetryCallback(callback)(&po.spo)
	}
}

// WithSessionQueueDeclarePassiveRetryCallback allows to set a custom retry callback for the session pool.
func WithSessionQueueDeclarePassiveRetryCallback(callback types.SessionRetryCallback) Option {
	return func(po *poolOption) {
		SessionPoolWithQueueDeclarePassiveRetryCallback(callback)(&po.spo)
	}
}

// WithSessionQueueDeleteRetryCallback allows to set a custom retry callback for the session pool.
func WithSessionQueueDeleteRetryCallback(callback types.SessionRetryCallback) Option {
	return func(po *poolOption) {
		SessionPoolWithQueueDeleteRetryCallback(callback)(&po.spo)
	}
}

// WithSessionQueueBindRetryCallback allows to set a custom retry callback for the session pool.
func WithSessionQueueBindRetryCallback(callback types.SessionRetryCallback) Option {
	return func(po *poolOption) {
		SessionPoolWithQueueBindRetryCallback(callback)(&po.spo)
	}
}

// WithSessionQueueUnbindRetryCallback allows to set a custom retry callback for the session pool.
func WithSessionQueueUnbindRetryCallback(callback types.SessionRetryCallback) Option {
	return func(po *poolOption) {
		SessionPoolWithQueueUnbindRetryCallback(callback)(&po.spo)
	}
}

// WithSessionQueuePurgeRetryCallback allows to set a custom retry callback for the session pool.
func WithSessionQueuePurgeRetryCallback(callback types.SessionRetryCallback) Option {
	return func(po *poolOption) {
		SessionPoolWithQueuePurgeRetryCallback(callback)(&po.spo)
	}
}

// WithSessionExchangeBindRetryCallback allows to set a custom retry callback for the session pool.
func WithSessionExchangeBindRetryCallback(callback types.SessionRetryCallback) Option {
	return func(po *poolOption) {
		SessionPoolWithExchangeBindRetryCallback(callback)(&po.spo)
	}
}

// WithSessionExchangeUnbindRetryCallback allows to set a custom retry callback for the session pool.
func WithSessionExchangeUnbindRetryCallback(callback types.SessionRetryCallback) Option {
	return func(po *poolOption) {
		SessionPoolWithExchangeUnbindRetryCallback(callback)(&po.spo)
	}
}

// WithSessionQoSRetryCallback allows to set a custom retry callback for the session pool.
func WithSessionQoSRetryCallback(callback types.SessionRetryCallback) Option {
	return func(po *poolOption) {
		SessionPoolWithQoSRetryCallback(callback)(&po.spo)
	}
}

// WithSessionFlowRetryCallback allows to set a custom retry callback for the session pool.
func WithSessionFlowRetryCallback(callback types.SessionRetryCallback) Option {
	return func(po *poolOption) {
		SessionPoolWithFlowRetryCallback(callback)(&po.spo)
	}
}
