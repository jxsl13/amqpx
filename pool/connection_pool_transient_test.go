package pool_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jxsl13/amqpx/internal/proxyutils"
	"github.com/jxsl13/amqpx/internal/testlogger"
	"github.com/jxsl13/amqpx/internal/testutils"
	"github.com/jxsl13/amqpx/pool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetTransientConnectionNoPanicOnFailedDerive verifies that GetTransientConnection
// does not panic with a nil pointer dereference when deriveConnection (NewConnection)
// fails and returns (nil, error).
//
// Before the fix, the code was:
//
//	conn, err = cp.deriveConnection(...)
//	if err == nil { return conn, nil }
//	err = conn.Recover(ctx)  // PANIC: conn is nil
//
// The fix wraps deriveConnection in a retry loop that checks ctx/shutdown first.
func TestGetTransientConnectionNoPanicOnFailedDerive(t *testing.T) {
	t.Parallel()

	var (
		ctx                      = context.TODO()
		log                      = testlogger.NewTestLogger(t)
		poolName                 = testutils.FuncName()
		proxyName, connectURL, _ = testutils.NextConnectURL()
	)

	// Create pool with proxy enabled (initial cached connections succeed)
	p, err := pool.NewConnectionPool(
		ctx,
		connectURL,
		1,
		pool.ConnectionPoolWithName(poolName),
		pool.ConnectionPoolWithLogger(log),
		pool.ConnectionPoolWithRecoverCallback(func(name string, retry int, err error) {
			log.Warn(fmt.Sprintf("recovering connection %s, attempt %d, error: %v", name, retry, err))
		}),
	)
	require.NoError(t, err)
	defer p.Close()

	// Disable the proxy so that NewConnection inside deriveConnection fails.
	proxy := proxyutils.NewProxy(t, proxyName)
	require.NoError(t, proxy.Disable())
	defer func() {
		require.NoError(t, proxy.Enable())
	}()

	// Call GetTransientConnection with a short timeout.
	// Before the fix: this would panic with nil pointer dereference.
	// After the fix: this returns a context.DeadlineExceeded error.
	tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	conn, err := p.GetTransientConnection(tctx)
	if conn == nil {
		assert.Error(t, err, "GetTransientConnection must never return a nil connection without an error")
	}
	assert.ErrorIs(t, err, context.DeadlineExceeded, "error should wrap context.DeadlineExceeded")
}

// TestGetTransientConnectionRecoveryOnReconnect verifies that the retry loop
// in GetTransientConnection eventually succeeds when the server becomes reachable again.
func TestGetTransientConnectionRecoveryOnReconnect(t *testing.T) {
	t.Parallel()

	var (
		ctx                      = context.TODO()
		log                      = testlogger.NewTestLogger(t)
		poolName                 = testutils.FuncName()
		proxyName, connectURL, _ = testutils.NextConnectURL()
	)

	// Create pool with proxy enabled
	p, err := pool.NewConnectionPool(
		ctx,
		connectURL,
		1,
		pool.ConnectionPoolWithName(poolName),
		pool.ConnectionPoolWithLogger(log),
		pool.ConnectionPoolWithRecoverCallback(func(name string, retry int, err error) {
			log.Warn(fmt.Sprintf("recovering connection %s, attempt %d, error: %v", name, retry, err))
		}),
	)
	require.NoError(t, err)
	defer p.Close()

	// Disable the proxy
	proxy := proxyutils.NewProxy(t, proxyName)
	require.NoError(t, proxy.Disable())

	// Re-enable after a short delay so the retry loop can succeed
	go func() {
		time.Sleep(3 * time.Second)
		if err := proxy.Enable(); err != nil {
			log.Error(fmt.Sprintf("failed to re-enable proxy: %v", err))
		}
		log.Info("proxy re-enabled")
	}()

	// GetTransientConnection should retry until the proxy comes back
	tctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	conn, err := p.GetTransientConnection(tctx)
	require.NoError(t, err, "should eventually succeed after proxy is re-enabled")
	require.NotNil(t, conn, "connection should not be nil")

	// Verify the connection is usable
	assert.NoError(t, conn.Recover(tctx), "connection should be healthy")

	p.ReturnConnection(conn, nil)
}
