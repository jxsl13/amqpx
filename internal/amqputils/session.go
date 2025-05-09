package amqputils

import (
	"context"
	"fmt"
	"testing"

	"github.com/jxsl13/amqpx/internal/testlogger"
	"github.com/jxsl13/amqpx/internal/testutils"
	"github.com/jxsl13/amqpx/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func NewSession(t *testing.T, ctx context.Context, connectURL, connectionName string, options ...types.ConnectionOption) (_ *types.Session, cleanup func()) {
	cleanup = func() {}
	log := testlogger.NewTestLogger(t)
	c, err := types.NewConnection(
		ctx,
		connectURL,
		connectionName,
		append([]types.ConnectionOption{
			types.ConnectionWithLogger(log),
		}, options...)...)
	if err != nil {
		require.NoError(t, err, "expected no error when creating new connection")
		return nil, cleanup
	}
	nextSessionName := testutils.SessionNameGenerator(connectionName)
	s, err := types.NewSession(
		c,
		nextSessionName(),
		types.SessionWithConfirms(true),
		types.SessionWithLogger(log),
		types.SessionWithRetryCallback(func(operation, connName, sessionName string, retry int, err error) {
			log.Info(fmt.Sprintf("retrying %s on connection %s, session %s, attempt %d, error: %s", operation, connName, sessionName, retry, err))
		}),
	)
	if err != nil {
		assert.NoError(t, err, "expected no error when creating new session")
		return nil, cleanup
	}
	return s, func() {
		log.Info(fmt.Sprintf("closing session %s", s.Name()))
		assert.NoError(t, s.Close(), "expected no error when closing session")
		log.Info(fmt.Sprintf("closing connection %s", c.Name()))
		assert.NoError(t, c.Close(), "expected no error when closing connection")
	}
}
