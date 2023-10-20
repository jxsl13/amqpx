package pool

import "github.com/rabbitmq/amqp091-go"

/*
	type Queue struct {
	    Name      string // server confirmed or generated name
	    Messages  int    // count of messages not awaiting acknowledgment
	    Consumers int    // number of consumers receiving deliveries
	}

Queue captures the current server state of the queue on the server returned from Channel.QueueDeclare or Channel.QueueInspect.
*/
type Queue amqp091.Queue
