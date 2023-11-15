package pool

// HandlerView is a read only snapshot of the current handler's configuration.
// This internal data structure is used in the corresponsing consumer.
type HandlerView struct {
	Queue       string
	HandlerFunc HandlerFunc
	ConsumeOptions
}
