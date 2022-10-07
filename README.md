# amqpx

[![Go Reference](https://pkg.go.dev/badge/github.com/jxsl13/amqpx.svg)](https://pkg.go.dev/github.com/jxsl13/amqpx)
[![Go Report Card](https://goreportcard.com/badge/github.com/jxsl13/amqpx)](https://goreportcard.com/report/github.com/jxsl13/amqpx)
[![GitHub go.mod Go version of a Go module](https://img.shields.io/github/go-mod/go-version/gomods/athens.svg)](https://github.com/gomods/athens)


`amqpx` is a robust and easy to use wrapper for `github.com/rabbitmq/amqp091-go`.

**Core features**
- connection pooling
- reconnect handling
- clean shutdown handling
- sane defaults
- robustness over performance (publisher & subscriber acks)

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