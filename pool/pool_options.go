package pool

import (
	"context"
	"crypto/tls"
	"math/rand"
	"os"
	"path/filepath"
	"time"
)

type poolOption struct {
	Name string
	Ctx  context.Context

	Size int

	ConnHeartbeatInterval time.Duration
	ConnTimeout           time.Duration
	TLSConfig             *tls.Config

	BackoffPolicy BackoffFunc

	SessionAckable    bool
	SessionBufferSize int
}

type PoolOption func(*poolOption)

func defaultAppName() string {
	appNameWithExt := filepath.Base(os.Args[0])
	ext := filepath.Ext(appNameWithExt)
	appNameWithoutExt := appNameWithExt[:len(appNameWithExt)-len(ext)]
	return appNameWithoutExt
}

// WithName gives all of your pooled connections a prefix name
func WithName(name string) PoolOption {
	if len(name) == 0 {
		name = defaultAppName()
	}
	return func(po *poolOption) {
		po.Name = name
	}
}

// WithHeartbeatInterval allows to set a custom heartbeat interval, that MUST be >= 1 * time.Second
func WithHeartbeatInterval(interval time.Duration) PoolOption {
	if interval < time.Second {
		interval = time.Second
	}
	return func(po *poolOption) {
		po.ConnHeartbeatInterval = interval
	}
}

// WithConnectionTimeout allows to set a custom connection timeout, that MUST be >= 1 * time.Second
func WithConnectionTimeout(timeout time.Duration) PoolOption {
	if timeout < time.Second {
		timeout = time.Second
	}
	return func(po *poolOption) {
		po.ConnTimeout = timeout
	}
}

// WithContext allows to set a custom connection timeout, that MUST be >= 1 * time.Second
func WithContext(ctx context.Context) PoolOption {
	if ctx == nil {
		panic("nil context passed")
	}
	return func(po *poolOption) {
		po.Ctx = ctx
	}
}

// WithTLS allows to configure tls connectivity.
func WithTLS(config *tls.Config) PoolOption {
	return func(po *poolOption) {
		po.TLSConfig = config
	}
}

func WithBackoffPolicy(poliy BackoffFunc) PoolOption {
	return func(po *poolOption) {
		po.BackoffPolicy = poliy
	}
}

type BackoffFunc func(retry int) (sleep time.Duration)

func newDefaultBackoffPolicy(min, max time.Duration) BackoffFunc {

	return func(retry int) (sleep time.Duration) {
		r := rand.New(rand.NewSource(time.Now().Unix()))

		wait := 2 << maxf(0, minf(32, retry)) * time.Second
		jitter := time.Duration(r.Int63n(int64(wait) / 5)) // max 20% jitter
		wait = min + wait + jitter
		if wait > max {
			wait = max
		}
		return wait
	}
}

func minf(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxf(a, b int) int {
	if a > b {
		return a
	}
	return b
}
