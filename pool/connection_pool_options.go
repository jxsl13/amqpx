package pool

import (
	"context"
	"crypto/tls"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/jxsl13/amqpx/logging"
)

type connectionPoolOption struct {
	Name string
	Ctx  context.Context

	Size int

	ConnHeartbeatInterval time.Duration
	ConnTimeout           time.Duration
	TLSConfig             *tls.Config

	Logger logging.Logger
}

type ConnectionPoolOption func(*connectionPoolOption)

func defaultAppName() string {
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

// ConnectionPoolWithContext allows to set a custom connection timeout, that MUST be >= 1 * time.Second
func ConnectionPoolWithContext(ctx context.Context) ConnectionPoolOption {
	if ctx == nil {
		panic("nil context passed")
	}
	return func(po *connectionPoolOption) {
		po.Ctx = ctx
	}
}

// ConnectionPoolWithTLS allows to configure tls connectivity.
func ConnectionPoolWithTLS(config *tls.Config) ConnectionPoolOption {
	return func(po *connectionPoolOption) {
		po.TLSConfig = config
	}
}

type BackoffFunc func(retry int) (sleep time.Duration)

func newDefaultBackoffPolicy(min, max time.Duration) BackoffFunc {

	return func(retry int) (sleep time.Duration) {
		r := rand.New(rand.NewSource(time.Now().Unix()))

		wait := 2 << maxi(0, mini(32, retry)) * time.Second
		jitter := time.Duration(r.Int63n(int64(wait) / 5)) // max 20% jitter
		wait = min + wait + jitter
		if wait > max {
			wait = max
		}
		return wait
	}
}

func mini(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxi(a, b int) int {
	if a > b {
		return a
	}
	return b
}
