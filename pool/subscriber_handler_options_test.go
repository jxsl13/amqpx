package pool

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWithMaxBatchSize(t *testing.T) {
	dummyHandler := func(context.Context, []Delivery) error { return nil }
	bh := NewBatchHandler("test", dummyHandler, WithMaxBatchSize(0), WithMaxBatchBytes(0))

	assert.Equal(t, defaultMaxBatchSize, bh.MaxBatchSize())
	assert.Equal(t, 0, bh.MaxBatchBytes())

	bh = NewBatchHandler("test", dummyHandler, WithMaxBatchBytes(0), WithMaxBatchSize(0))
	assert.Equal(t, defaultMaxBatchSize, bh.MaxBatchSize())
	assert.Equal(t, 0, bh.MaxBatchBytes())

	bh = NewBatchHandler("test", dummyHandler, WithMaxBatchBytes(1), WithMaxBatchSize(1))
	assert.Equal(t, 1, bh.MaxBatchSize())
	assert.Equal(t, 1, bh.MaxBatchBytes())

	// if you want to set specific limits to infinite, you may first set all the != 0 options and then set the
	// rest of the options to 0.
	bh = NewBatchHandler("test", dummyHandler, WithMaxBatchBytes(50), WithMaxBatchSize(0))
	assert.Equal(t, 0, bh.MaxBatchSize())
	assert.Equal(t, 50, bh.MaxBatchBytes())
}
