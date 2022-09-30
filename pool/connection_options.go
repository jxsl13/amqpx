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

// ConnectionWithTimeout allows to set a custom connection timeout, that MUST be >= 1 * time.Second
func ConnectionWithTimeout(timeout time.Duration) ConnectionOption {
	if timeout < time.Second {
		timeout = time.Second
	}
	return func(co *connectionOption) {
		co.ConnectionTimeout = timeout
	}
}

// ConnectionWithBackoffPolicy influences the sleep interval between connection recovery retries.
func ConnectionWithBackoffPolicy(policy BackoffFunc) ConnectionOption {
	return func(co *connectionOption) {
		co.BackoffPolicy = policy
	}
}

// ConnectionWithContext allows to set a custom connection timeout, that MUST be >= 1 * time.Second
func ConnectionWithContext(ctx context.Context) ConnectionOption {
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
