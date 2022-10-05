package pool

var (
	// QuorumArgs is the argument you need to pass in order to create a quorum queue.
	QuorumArgs = Table{
		"x-queue-type": "quorum",
	}
)

const (
	// DeadLetterExchangeKey can be used in order to create a dead letter exchange
	// https://www.rabbitmq.com/dlx.html
	DeadLetterExchangeKey = "x-dead-letter-exchange"
)
