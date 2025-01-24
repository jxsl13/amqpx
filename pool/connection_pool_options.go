package pool

import (
	"context"
	"crypto/tls"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"time"

	"github.com/jxsl13/amqpx/logging"
)

type connectionPoolOption struct {
	Name string
	Ctx  context.Context

	Capacity int

	ConnHeartbeatInterval time.Duration
	ConnTimeout           time.Duration
	TLSConfig             *tls.Config

	Logger logging.Logger

	ConnectionRecoverCallback ConnectionRecoverCallback
}

type ConnectionPoolOption func(*connectionPoolOption)

func defaultAppName() string {

	if bi, ok := debug.ReadBuildInfo(); ok && bi.Path != "" {
		parts := strings.Split(bi.Path, "/")
		if len(parts) > 0 {
			return parts[len(parts)-1]
		}
	}

	// fallback
	appNameWithExt := filepath.Base(os.Args[0])
	ext := filepath.Ext(appNameWithExt)
	appNameWithoutExt := appNameWithExt[:len(appNameWithExt)-len(ext)]
	return appNameWithoutExt

}

// ConnectionPoolWithLogger allows to set a custom logger.
func ConnectionPoolWithLogger(logger logging.Logger) ConnectionPoolOption {
	return func(po *connectionPoolOption) {
		po.Logger = logger
	}
}

// ConnectionPoolWithName gives all of your pooled connections a prefix name
func ConnectionPoolWithName(name string) ConnectionPoolOption {
	if len(name) == 0 {
		name = defaultAppName()
	}
	return func(po *connectionPoolOption) {
		po.Name = name
	}
}

// ConnectionPoolWithNameSuffix adds a suffix to the connection pool name
func ConnectionPoolWithNameSuffix(suffix string) ConnectionPoolOption {
	return func(po *connectionPoolOption) {
		po.Name += suffix
	}
}

// ConnectionPoolWithNamePrefix adds a prefix to the connection pool name
func ConnectionPoolWithNamePrefix(prefix string) ConnectionPoolOption {
	return func(po *connectionPoolOption) {
		po.Name = prefix + po.Name
	}
}

// WithHeartbeatInterval allows to set a custom heartbeat interval, that MUST be >= 1 * time.Second
func ConnectionPoolWithHeartbeatInterval(interval time.Duration) ConnectionPoolOption {
	if interval < time.Second {
		interval = time.Second
	}
	return func(po *connectionPoolOption) {
		po.ConnHeartbeatInterval = interval
	}
}

// ConnectionPoolWithConnectionTimeout allows to set a custom connection timeout, that MUST be >= 1 * time.Second
func ConnectionPoolWithConnectionTimeout(timeout time.Duration) ConnectionPoolOption {
	if timeout < time.Second {
		timeout = time.Second
	}
	return func(po *connectionPoolOption) {
		po.ConnTimeout = timeout
	}
}

// ConnectionPoolWithTLS allows to configure tls connectivity.
func ConnectionPoolWithTLS(config *tls.Config) ConnectionPoolOption {
	return func(po *connectionPoolOption) {
		po.TLSConfig = config
	}
}

// ConnectionPoolWithRecoverCallback allows to set a custom recover callback.
func ConnectionPoolWithRecoverCallback(callback ConnectionRecoverCallback) ConnectionPoolOption {
	return func(po *connectionPoolOption) {
		po.ConnectionRecoverCallback = callback
	}
}

type BackoffFunc func(retry int) (sleep time.Duration)

func newDefaultBackoffPolicy(minDuration, maxDuration time.Duration) BackoffFunc {

	factor := time.Second
	for _, scale := range []time.Duration{time.Hour, time.Minute, time.Second, time.Millisecond, time.Microsecond, time.Nanosecond} {
		d := minDuration.Truncate(scale)
		if d > 0 {
			factor = scale
			break
		}
	}

	return func(retry int) (sleep time.Duration) {

		wait := 2 << max(0, min(32, retry)) * factor                     // 2^(min(32, retry)) * factor (second, min, hours, etc)
		jitter := time.Duration(rand.Int64N(int64(max(1, int(wait)/5)))) // max 20% jitter
		wait = minDuration + wait + jitter
		if wait > maxDuration {
			wait = maxDuration
		}
		return wait
	}
}
