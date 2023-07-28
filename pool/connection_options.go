package pool

import (
	"context"
	"crypto/tls"
	"time"

	"github.com/jxsl13/amqpx/logging"
)

type connectionOption struct {
	Logger            logging.Logger
	Cached            bool
	HeartbeatInterval time.Duration
	ConnectionTimeout time.Duration
	BackoffPolicy     BackoffFunc
	Ctx               context.Context
	TLSConfig         *tls.Config

	SlowClose bool // set to true for goleak tests
}

type ConnectionOption func(*connectionOption)

// ConnectionWithLogger allows to set a logger. By default no logger is set.
func ConnectionWithLogger(logger logging.Logger) ConnectionOption {
	return func(co *connectionOption) {
		co.Logger = logger
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

// ConnectionWithSlowClose is only needed for integration tests.
// It waits for standard library tcp connection goroutines to properly timeout.
// So that we don't get false positives in our leak tests.
// Set to true in order to wait for dangling goroutines to timeout before closing the connection.
func ConnectionWithSlowClose(slowClose bool) ConnectionOption {
	return func(co *connectionOption) {
		co.SlowClose = slowClose
	}
}
