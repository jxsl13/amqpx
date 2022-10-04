package pool

var (
	// QuorumArgs is the argument you need to pass in order to create a quorum queue.
	QuorumArgs = Table{
		"x-queue-type": "quorum",
	}
)
