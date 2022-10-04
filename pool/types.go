package pool

import amqp "github.com/rabbitmq/amqp091-go"

type (
	Delivery     = amqp.Delivery
	DeliveryTag  = uint64
	Confirmation = amqp.Confirmation
	Publishing   = amqp.Publishing
	Table        = amqp.Table
)
