package types

import (
	"context"
	"crypto/tls"
	"log/slog"
	"time"
)

type connectionOption struct {
	Logger            *slog.Logger
	Cached            bool
	HeartbeatInterval time.Duration
	ConnectionTimeout time.Duration
	BackoffPolicy     BackoffFunc
	Ctx               context.Context
	TLSConfig         *tls.Config
	RecoverCallback   ConnectionRecoverCallback
}

type ConnectionOption func(*connectionOption)

// ConnectionWithLogger allows to set a logger. By default no logger is set.
func ConnectionWithLogger(logger *slog.Logger) ConnectionOption {
	return func(co *connectionOption) {
		if logger != nil {
			co.Logger = logger
		}
	}
}

// ConnectionHeartbeatInterval allows to set a custom heartbeat interval, that MUST be >= 1 * time.Second
func ConnectionWithHeartbeatInterval(interval time.Duration) ConnectionOption {
	if interval < time.Second {
		interval = time.Second
	}
	return func(co *connectionOption) {
		co.HeartbeatInterval = interval
	}
}

// ConnectionWithCached makes a connection a cached connection
// This is only necessary for the connection pool, as cached connections are part of a pool
// and can be returned back to the pool without being closed.
func ConnectionWithCached(cached bool) ConnectionOption {
	return func(co *connectionOption) {
		co.Cached = cached
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

// ConnectionWithTLS allows to configure tls connectivity.
func ConnectionWithTLS(config *tls.Config) ConnectionOption {
	return func(co *connectionOption) {
		co.TLSConfig = config
	}
}

// ConnectionWithRecoverCallback allows to set a custom recover callback.
func ConnectionWithRecoverCallback(callback ConnectionRecoverCallback) ConnectionOption {
	return func(co *connectionOption) {
		co.RecoverCallback = callback
	}
}
