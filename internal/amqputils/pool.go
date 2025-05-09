package amqputils

import (
	"context"
	"testing"

	"github.com/jxsl13/amqpx/internal/testlogger"
	"github.com/jxsl13/amqpx/pool"
	"github.com/stretchr/testify/assert"
)

func NewPool(t *testing.T, ctx context.Context, connectURL, poolName string, numConns, numSessions int) *pool.Pool {
	p, err := pool.New(
		ctx,
		connectURL,
		numConns,
		numSessions,
		pool.WithLogger(testlogger.NewTestLogger(t)),
		pool.WithConfirms(true),
		pool.WithName(poolName),
	)
	if err != nil {
		assert.NoError(t, err)
		return nil
	}
	return p
}
