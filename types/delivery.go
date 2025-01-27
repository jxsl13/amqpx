package types

import (
	"time"

	"github.com/rabbitmq/amqp091-go"
)

/*
Delivery captures the fields for a previously delivered message resident in
a queue to be delivered by the server to a consumer from Channel.Consume or
Channel.Get.

	type Delivery struct {
		Headers Table // Application or header exchange table

		// Properties
		ContentType     string    // MIME content type
		ContentEncoding string    // MIME content encoding
		DeliveryMode    uint8     // queue implementation use - non-persistent (1) or persistent (2)
		Priority        uint8     // queue implementation use - 0 to 9
		CorrelationId   string    // application use - correlation identifier
		ReplyTo         string    // application use - address to reply to (ex: RPC)
		Expiration      string    // implementation use - message expiration spec
		MessageId       string    // application use - message identifier
		Timestamp       time.Time // application use - message timestamp
		Type            string    // application use - message type name
		UserId          string    // application use - creating user - should be authenticated user
		AppId           string    // application use - creating application id

		// Valid only with Channel.Consume
		ConsumerTag string

		// Valid only with Channel.Get
		MessageCount uint32

		DeliveryTag uint64
		Redelivered bool
		Exchange    string // basic.publish exchange
		RoutingKey  string // basic.publish routing key

		Body []byte
	}
*/

func NewDeliveryFromAMQP091(delivery amqp091.Delivery) Delivery {
	return Delivery{
		Headers:         Table(delivery.Headers),
		ContentType:     delivery.ContentType,
		ContentEncoding: delivery.ContentEncoding,
		DeliveryMode:    delivery.DeliveryMode,
		Priority:        delivery.Priority,
		CorrelationId:   delivery.CorrelationId,
		ReplyTo:         delivery.ReplyTo,
		Expiration:      delivery.Expiration,
		MessageId:       delivery.MessageId,
		Timestamp:       delivery.Timestamp,
		Type:            delivery.Type,
		UserId:          delivery.UserId,
		AppId:           delivery.AppId,
		ConsumerTag:     delivery.ConsumerTag,
		MessageCount:    delivery.MessageCount,
		DeliveryTag:     delivery.DeliveryTag,
		Redelivered:     delivery.Redelivered,
		Exchange:        delivery.Exchange,
		RoutingKey:      delivery.RoutingKey,
		Body:            delivery.Body,
	}
}

type Delivery struct {
	// Contains exactly the same fields as amqp091.Delivery except for the Acknowledger field

	Headers Table // Application or header exchange table

	// Properties
	ContentType     string    // MIME content type
	ContentEncoding string    // MIME content encoding
	DeliveryMode    uint8     // queue implementation use - non-persistent (1) or persistent (2)
	Priority        uint8     // queue implementation use - 0 to 9
	CorrelationId   string    // application use - correlation identifier
	ReplyTo         string    // application use - address to reply to (ex: RPC)
	Expiration      string    // implementation use - message expiration spec
	MessageId       string    // application use - message identifier
	Timestamp       time.Time // application use - message timestamp
	Type            string    // application use - message type name
	UserId          string    // application use - creating user - should be authenticated user
	AppId           string    // application use - creating application id

	// Valid only with Channel.Consume
	ConsumerTag string

	// Valid only with Channel.Get
	MessageCount uint32

	DeliveryTag uint64
	Redelivered bool
	Exchange    string // basic.publish exchange
	RoutingKey  string // basic.publish routing key

	Body []byte
}
