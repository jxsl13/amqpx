package amqputils

import (
	"context"
	"sync"
	"testing"

	"github.com/jxsl13/amqpx/pool"
	"github.com/jxsl13/amqpx/types"
	"github.com/stretchr/testify/assert"
)

func PublisherPublishN(t *testing.T, ctx context.Context, p *pool.Pool, exchangeName string, publishMessageGenerator func() string, n int) {
	pub := pool.NewPublisher(p)
	defer pub.Close()

	for i := 0; i < n; i++ {
		message := publishMessageGenerator()
		err := pub.Publish(ctx, exchangeName, "", types.Publishing{
			ContentType: "text/plain",
			Body:        []byte(message),
		})
		assert.NoError(t, err)
	}
}

func PublisherPublishAsyncN(
	t *testing.T,
	ctx context.Context,
	wg *sync.WaitGroup,
	p *pool.Pool,
	exchangeName string,
	publishMessageGenerator func() string,
	n int,
) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		PublisherPublishN(t, ctx, p, exchangeName, publishMessageGenerator, n)
	}()
}
