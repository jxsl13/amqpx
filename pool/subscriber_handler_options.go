package pool

import "time"

type BatchHandlerOption func(*BatchHandler)

// WithMaxBatchSize sets the maximum size of a batch.
// If set to 0 the batch size is not limited.
// This means that the batch size is only limited by the maximum batch size in bytes.
func WithMaxBatchSize(size int) BatchHandlerOption {
	return func(bh *BatchHandler) {

		switch {
		case and(size <= 0, bh.maxBatchBytes == 0):
			// we need to set a sane default
			bh.maxBatchSize = defaultMaxBatchSize
		case and(size <= 0, bh.maxBatchBytes > 0):
			// we need to set the batch size to unlimited
			// because the batch is limited by bytes
			bh.maxBatchSize = 0
		case size > 0:
			// we need to set the batch size to the provided value
			bh.maxBatchSize = size
		}
	}
}

// WithMaxBatchBytes sets the maximum size of a batch in bytes.
// If the batch size exceeds this limit, the batch is passed to the handler function.
// If the value is set to 0, the batch size is not limited by bytes.
func WithMaxBatchBytes(size int) BatchHandlerOption {
	return func(bh *BatchHandler) {
		switch {
		case and(size <= 0, bh.maxBatchSize == 0):
			// do not change the current value
			return
		case and(size <= 0, bh.maxBatchSize > 0):
			// we need to set the batch size to unlimited
			// because the batch is limited by number of messages
			bh.maxBatchBytes = 0
		case size > 0:
			// we need to set the batch size to the provided value
			bh.maxBatchBytes = size
		}
	}
}

func WithBatchFlushTimeout(d time.Duration) BatchHandlerOption {
	return func(bh *BatchHandler) {
		if d <= 0 {
			bh.flushTimeout = defaultFlushTimeout
		} else {
			bh.flushTimeout = d
		}
	}
}

func WithBatchConsumeOptions(opts ConsumeOptions) BatchHandlerOption {
	return func(bh *BatchHandler) {
		bh.consumeOpts = opts
	}
}

func and(b ...bool) bool {
	for _, v := range b {
		if !v {
			return false
		}
	}
	return true
}
