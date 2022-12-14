package pool

import (
	"context"
	"crypto/tls"
	"time"

	"github.com/jxsl13/amqpx/logging"
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

// WithContext allows to set a custom connection timeout, that MUST be >= 1 * time.Second
func WithContext(ctx context.Context) Option {
	return func(po *poolOption) {
		ConnectionPoolWithContext(ctx)(&po.cpo)
	}
}

// WithTLS allows to configure tls connectivity.
func WithTLS(config *tls.Config) Option {
	return func(po *poolOption) {
		ConnectionPoolWithTLS(config)(&po.cpo)
	}
}

// WithBufferSize allows to configurethe size of
// the confirmation, error & blocker buffers of all sessions
func WithBufferSize(size int) Option {
	return func(po *poolOption) {
		SessionPoolWithBufferSize(size)(&po.spo)
	}
}

// WithConfirms requires all messages from sessions to be acked.
func WithConfirms(requirePublishConfirms bool) Option {
	return func(po *poolOption) {
		SessionPoolWithConfirms(requirePublishConfirms)(&po.spo)
	}
}
