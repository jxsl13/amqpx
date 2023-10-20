package pool

import "github.com/rabbitmq/amqp091-go"

/*
Delivery captures the fields for a previously delivered message resident in
a queue to be delivered by the server to a consumer from Channel.Consume or
Channel.Get.

	type Delivery struct {
		Acknowledger Acknowledger // the channel from which this delivery arrived

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
type Delivery = amqp091.Delivery
