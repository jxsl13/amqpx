# amqpx

[![GitHub license](https://badgen.net/github/license/jxsl13/amqpx)](https://pkg.go.dev/github.com/jxsl13/amqpx/blob/master/LICENSE)
[![Go Reference](https://pkg.go.dev/badge/github.com/jxsl13/amqpx.svg)](https://pkg.go.dev/github.com/jxsl13/amqpx)
[![Go Report Card](https://goreportcard.com/badge/github.com/jxsl13/amqpx)](https://goreportcard.com/report/github.com/jxsl13/amqpx)
[![GitHub go.mod Go version of a Go module](https://img.shields.io/github/go-mod/go-version/jxsl13/amqpx.svg)](https://github.com/jxsl13/amqpx)
[![GitHub latest release](https://badgen.net/github/tag/jxsl13/amqpx)](https://github.com/jxsl13/amqpx/tags)


`amqpx` is a robust and easy to use wrapper for `github.com/rabbitmq/amqp091-go`.

**Core features**
- connection pooling
- reconnect handling
- clean shutdown handling
- sane defaults
- robustness over performance by default (publisher & subscriber acks)
- every default can be changed to your liking

This library is highly inspired by `https://github.com/houseofcat/turbocookedrabbit`

## Getting started

```shell
go get github.com/jxsl13/amqpx
```

Example
```go
package main

import (
	"context"
	"fmt"
	"os/signal"

	"github.com/jxsl13/amqpx"
	"github.com/jxsl13/amqpx/logging"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background())
	defer cancel()

	amqpx.RegisterTopologyCreator(func(t *amqpx.Topologer) error {
		// error handling omitted for brevity
		t.ExchangeDeclare("example-exchange", "topic", true, false, false, false, nil)
		t.QueueDeclare("example-queue", true, false, false, false, amqpx.QuorumQueue)
		t.QueueBind("example-queue", "route.name.v1", "example-exchange", false, nil)
		return nil
	})
	amqpx.RegisterTopologyDeleter(func(t *amqpx.Topologer) error {
		// error handling omitted for brevity
		t.QueueDelete("example-queue", false, false, false)
		t.ExchangeDelete("example-exchange", false, false)
		return nil
	})

	amqpx.RegisterHandler("example-queue", "", false, false, false, false, nil, func(msg amqpx.Delivery) error {
		fmt.Println("received message:", string(msg.Body))
		fmt.Println("canceling context")
		cancel()

		// return error for nack + retry
		return nil
	})

	amqpx.Start(
		amqpx.NewURL("localhost", 5672, "admin", "password"), // or amqp://username@password:localhost:5672
		amqpx.WithLogger(logging.NewNoOpLogger()),            // provide a logger that implements the logging.Logger interface (logrus adapter is provided)
	)
	defer amqpx.Close()

	amqpx.Publish("example-exchange", "route.name.v1", false, false, amqpx.Publishing{
		ContentType: "application/json",
		Body:        []byte("my test event"),
	})

	<-ctx.Done()
}

```

## Types

The `amqpx` package provides a single type which incoorporates everything needed for consuming and publishing messages.

The `pool` package provides all of the implementation details .

### amqpx.AMQPX

The `AMQPX` struct consists at least one connection pool, a `Publisher`, a `Subscriber` and a `Topologer`.
Upon `Start(..)` and upon `Close()` a `Topologer` is created which creates the topology or destroys a topology based on one or *multiple* functions that were registered via `RegisterTopologyCreator` or `RegisterTopologyDeleter`.
After the topology has been created, a `Publisher` is instantiated from a publisher connection and session `Pool`. 
The `Publisher` can be used to publish messages to specific *exchanges* with a given *routing key*.
In case you register an event handler function via `RegisterHandler`, then another connection and session `Pool` is created which is then used to instantiate a `Subscriber`. The `Subscriber` communicates via one or multiple separate TCP connections in order to prevent interference between the `Publisher` and `Subscriber`.

The `amqpx` package defines a global variable that allows the package `amqpx` to be used like the `AMQPX` object.

### pool.Topologer

The `Topologer` allows to create, delete, bind or unbind *exchanges* or *queues*

### pool.Publisher

The `Publisher` allows to publish individual events or messages to *exchanges* with a given *routing key*.

### pool.Subscriber

The `Subscriber` allows to register event handler functions that *consume messages from individual queues*. 
A `Subscriber` must be `Start()`ed in order for it to create consumer goroutines that process events from broker queues.

## Development

Test flags you might want to add:
```shell
-v -race -count=1
```
- see test logs
- detect data races
- do not cache test results

Starting the test environment:
```shell
docker-compose up -d
```

Starting the tests:
```shell
go test -v -race -count=1 ./...
```