package pool_test

import (
	"testing"

	"github.com/jxsl13/amqpx/pool"
	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {

	p, err := pool.New("amqp://admin:password@localhost:5672", 10, pool.WithName("TestNew"))
	if err != nil {
		assert.NoError(t, err)
		return
	}
	defer p.Close()

	session, err := p.GetSession()
	if err != nil {
		assert.NoError(t, err)
		return
	}

	p.ReturnSession(session, false)

}
