package pool

import "time"

type BatchHandlerOption func(*BatchHandler)

func WithMaxBatchSize(size int) BatchHandlerOption {
	return func(bh *BatchHandler) {
		if size <= 0 {
			bh.maxBatchSize = defaultMaxBatchSize
		} else {
			bh.maxBatchSize = size
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
