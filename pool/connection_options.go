package pool

import (
	"context"
	"crypto/tls"
	"time"
)

type connectionOption struct {
	HeartbeatInterval time.Duration
	ConnectionTimeout time.Duration
	BackoffPolicy     BackoffFunc
	Ctx               context.Context
	TLSConfig         *tls.Config
}

type ConnectionOption func(*connectionOption)

// ConnectionHeartbeatInterval allows to set a custom heartbeat interval, that MUST be >= 1 * time.Second
func ConnectionHeartbeatInterval(interval time.Duration) ConnectionOption {
	if interval < time.Second {
		interval = time.Second
	}
	return func(co *connectionOption) {
		co.HeartbeatInterval = interval
	}
}

// ConnectionTimeout allows to set a custom connection timeout, that MUST be >= 1 * time.Second
func ConnectionTimeout(timeout time.Duration) ConnectionOption {
	if timeout < time.Second {
		timeout = time.Second
	}
	return func(co *connectionOption) {
		co.ConnectionTimeout = timeout
	}
}

// ConnectionBackoffPolicy influences the sleep interval between connection recovery retries.
func ConnectionBackoffPolicy(policy BackoffFunc) ConnectionOption {
	return func(co *connectionOption) {
		co.BackoffPolicy = policy
	}
}

// ConnectionContext allows to set a custom connection timeout, that MUST be >= 1 * time.Second
func ConnectionContext(ctx context.Context) ConnectionOption {
	if ctx == nil {
		panic("nil context passed")
	}
	return func(co *connectionOption) {
		co.Ctx = ctx
	}
}

// ConnectionWithTLS allows to configure tls connectivity.
func ConnectionWithTLS(config *tls.Config) ConnectionOption {
	return func(co *connectionOption) {
		co.TLSConfig = config
	}
}
