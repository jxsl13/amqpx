package types

import (
	"slices"

	"github.com/rabbitmq/amqp091-go"
)

var (
	QuorumQueue = Table{
		"x-queue-type": "quorum",
	}
)

/*
	Table is a dynamic map of arguments that may be passed additionally to functions.

	type Table map[string]interface{}

Table stores user supplied fields of the following types:

	bool
	byte
	int8
	float32
	float64
	int
	int16
	int32
	int64
	nil
	string
	time.Time
	amqp.Decimal
	amqp.Table
	[]byte
	[]interface{} - containing above types

Functions taking a table will immediately fail when the table contains a value of an unsupported type.

The caller must be specific in which precision of integer it wishes to
encode.

Use a type assertion when reading values from a table for type conversion.

RabbitMQ expects int32 for integer values.
*/
type Table amqp091.Table

func NewTable() Table {
	return make(Table)
}

// This option is interesting for quorum queues.
// It specifies the number f redeliveries that still presereve the order of the messages. After that the order is not quaranteed anymore.
func (t Table) WithDeliveryLimit(limit int) Table {
	m := t.clone(1)
	m["x-delivery-limit"] = limit
	return t
}

// Rejected messages will be routed to the dead-letter exchange.
// Which in turn routes them to some specified queue.
func (t Table) WithDeadLetterExchange(exchange string) Table {
	m := t.clone(1)
	m["x-dead-letter-exchange"] = exchange
	return t
}

// Rejected messages will be routed to the dead-letter exchange.
// Which in turn routes them to using the specified routing key.
func (t Table) WithDeadLetterExchangeAndRoutingKey(exchange, routingKey string) Table {
	m := t.clone(2)
	m["x-dead-letter-exchange"] = exchange
	m["x-dead-letter-routing-key"] = routingKey
	return t
}

// Returns the number of deliveries of the message.
func (t Table) DeliveryCount() (int64, bool) {
	if t == nil {
		return 0, false
	}
	v, ok := t["x-delivery-count"]
	if !ok {
		return 0, false
	}

	cnt, ok := v.(int64)
	if !ok {
		return 0, false
	}

	return cnt, true
}

func (t Table) Death() (int64, bool) {
	if t == nil {
		return 0, false
	}

	v, ok := t["x-death"]
	if !ok {
		return 0, false
	}

	death, ok := v.(int64)
	if !ok {
		return 0, false
	}
	return death, true

}

func (t Table) Clone() Table {
	return t.clone()
}

func (t Table) clone(add ...int) Table {

	inc := 0
	if len(add) > 0 && add[0] > 0 {
		inc = add[0]
	}
	m := make(Table, len(t)+inc)
	for k, v := range t {
		if vv, ok := v.([]byte); ok {
			m[k] = slices.Clone(vv)
		} else if vv, ok := v.([]interface{}); ok {
			m[k] = slices.Clone(vv)
		} else {
			m[k] = v
		}
	}
	return m
}
