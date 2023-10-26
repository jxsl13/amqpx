package pool

// handlerView is a read only snapshot of the current handler's configuration and runtime state.
// This internal data structure is used in the corresponsing consumer.
type handlerView struct {
	// called in the consumer function & wrapper
	pausing done
	paused  cancel

	resuming done
	resumed  cancel

	Queue       string
	HandlerFunc HandlerFunc
	ConsumeOptions
}
