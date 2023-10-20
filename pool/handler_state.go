package pool

type HandlerState string

const (
	Pausing  HandlerState = "pausing"
	Paused   HandlerState = "paused"
	Starting HandlerState = "starting"
	Running  HandlerState = "running"

	// Used internally
	resuming = Starting
	resumed  = Running
)
