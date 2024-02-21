package pool

// ConnectionRecoverCallback is a function that can be called after a connection failed to be established
// and is about to be recovered.
type ConnectionRecoverCallback func(name string, retry int, err error)

// RetryCallback is a function that is called when some operation fails.
type SessionRetryCallback func(operation, connName, sessionName string, retry int, err error)
