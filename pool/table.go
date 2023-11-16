package pool

import "github.com/rabbitmq/amqp091-go"

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
type Table = amqp091.Table
