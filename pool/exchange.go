package pool

type ExchangeKind string

const (
	/*
		The first RabbitMQ exchange type, the direct exchange, uses a message routing key to transport messages to queues.
		The routing key is a message attribute that the producer adds to the message header. You can consider the routing
		key to be an “address” that the exchange uses to determine how the message should be routed. A message is delivered
		to the queue with the binding key that exactly matches the message’s routing key.

		The direct exchange’s default exchange is “amq. direct“, which AMQP brokers must offer for communication.
		As is shown in the figure, queue A (create_pdf_queue) is tied to a direct exchange (pdf_events) with the binding
		key “pdf_create”. When a new message arrives at the direct exchange with the routing key “pdf_create”, the exchange
		sends it to the queue where the binding key = routing key; which is queue A in this example (create_pdf_queue).
	*/
	ExchangeKindDirect ExchangeKind = "direct"

	/*
		A fanout exchange, like direct and topic exchange, duplicates and routes a received message to any associated queues,
		regardless of routing keys or pattern matching. Here, your provided keys will be entirely ignored.

		Fanout exchanges are useful when the same message needs to be passed to one or perhaps more queues with consumers who may
		process the message differently. As shown in the image, a message received by the fanout exchange is copied and routed to all
		three queues associated with the exchange. When something happens, such as a sporting event or weather forecast, all connected
		mobile devices will be notified. For the fanout RabbitMQ exchange type, “amq.fanout” is the default exchange that must be provided
		by AMQP brokers.
	*/
	ExchangeKindFanOut ExchangeKind = "fanout"

	/*
		Topic RabbitMQ exchange type sends messages to queues depending on wildcard matches between the routing key and the queue binding’s routing pattern.
		Messages are routed to one or more queues based on a pattern that matches a message routing key. A list of words separated by a period must be used
		as the routing key (.).

		The routing patterns may include an asterisk (“*”) to match a word in a specified position of the routing key (for example, a routing pattern of
		“agreements.*.*.b.*” only matches routing keys with “agreements” as the first word and “b” as the fourth word).
		A pound symbol (“#”) denotes a match of zero or more words.

		In topic exchange, consumers indicate which topics are of interest to them. The consumer establishes a queue and binds it to the exchange using a
		certain routing pattern. All messages with a routing key that matches the routing pattern are routed to the queue, where they will remain until
		the consumer consumes them. For the topic RabbitMQ exchange type, “amq.topic” is the default topic exchange that AMQP brokers must provide for
		message exchange.
	*/
	ExchangeKindTopic ExchangeKind = "topic"

	/*
		A headers RabbitMQ exchange type is a message routing system that uses arguments with headers and optional values to route messages.
		Header exchanges are identical to topic exchanges, except that instead of using routing keys, messages are routed based on header values.
		If the value of the header equals the value of supply during binding, the message matches.

		In the binding between exchange and queue, a specific argument termed “x-match” indicates whether all headers must match or only one.
		For the message to match, any common header between the message and the binding should match, or all of the headers referenced in the
		binding must be present in the message.

		The “x-match” property has two possible values: “any” and “all,” with “all” being the default. A value of “all” indicates that all
		header pairs (key, value) must match, whereas “any” indicates that at least one pair must match. Instead of a string, headers can be
		built with a larger range of data types, such as integers or hashes. The headers exchange type (when used with the binding option “any”)
		is useful for steering messages containing a subset of known (unordered) criteria.

		For the header RabbitMQ exchange type, “amq.headers” is the default topic exchange that AMQP brokers must supply.
	*/
	ExchangeKindHeaders ExchangeKind = "headers"
)

const (
	/*
		ExchangeKeyDeadLetter can be used in order to create a dead letter exchange (reference: https://www.rabbitmq.com/dlx.html)
		Some queue consumers may be unable to process certain alerts, and the queue itself may reject messages as a result of certain events.
		For instance, a message is dropped if there is no matching queue for it. In that instance, Dead Letter Exchanges must be implemented
		so that those messages can be saved and reprocessed later. The “Dead Letter Exchange” is an AMQP enhancement provided by RabbitMQ.
		This exchange has the capability of capturing messages that are not deliverable.
	*/
	ExchangeKeyDeadLetter = "x-dead-letter-exchange"
)
