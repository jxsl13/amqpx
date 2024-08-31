# amqpx

[![GitHub license](https://badgen.net/github/license/jxsl13/amqpx)](https://pkg.go.dev/github.com/jxsl13/amqpx/blob/master/LICENSE)
[![Go Reference](https://pkg.go.dev/badge/github.com/jxsl13/amqpx.svg)](https://pkg.go.dev/github.com/jxsl13/amqpx)
[![Go Report Card](https://goreportcard.com/badge/github.com/jxsl13/amqpx)](https://goreportcard.com/report/github.com/jxsl13/amqpx)
[![codecov](https://codecov.io/github/jxsl13/amqpx/branch/main/graph/badge.svg?token=bQuDbBFzMm)](https://codecov.io/github/jxsl13/amqpx)
[![GitHub go.mod Go version of a Go module](https://img.shields.io/github/go-mod/go-version/jxsl13/amqpx.svg)](https://github.com/jxsl13/amqpx)
[![GitHub latest release](https://badgen.net/github/tag/jxsl13/amqpx)](https://github.com/jxsl13/amqpx/tags)


`amqpx` is a robust and easy to use wrapper for `github.com/rabbitmq/amqp091-go`.

**Core features**
- connection & session (channel) pooling
- reconnect handling
- batch processing
- pause/resume consumers
- clean shutdown handling
- sane defaults
- resilience & robustness over performance by default (publisher & subscriber acks)
- every default can be changed to your liking

This library is highly inspired by `https://github.com/houseofcat/turbocookedrabbit`

## Getting started

```shell
go get github.com/jxsl13/amqpx@latest
```

### Example
```go
package main

import (
	"context"
	"fmt"
	"os/signal"
	"syscall"

	"github.com/jxsl13/amqpx"
	"github.com/jxsl13/amqpx/logging"
	"github.com/jxsl13/amqpx/pool"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	amqpx.RegisterTopologyCreator(func(ctx context.Context, t *pool.Topologer) error {
		// error handling omitted for brevity
		_ = t.ExchangeDeclare(ctx, "example-exchange", "topic") // durable exchange by default
		_, _ = t.QueueDeclare(ctx, "example-queue")             // durable quorum queue by default
		_ = t.QueueBind(ctx, "example-queue", "route.name.v1.event", "example-exchange")
		return nil
	})
	amqpx.RegisterTopologyDeleter(func(ctx context.Context, t *pool.Topologer) error {
		// error handling omitted for brevity
		_, _ = t.QueueDelete(ctx, "example-queue")
		_ = t.ExchangeDelete(ctx, "example-exchange")
		return nil
	})

	amqpx.RegisterHandler("example-queue", func(ctx context.Context, msg pool.Delivery) error {
		fmt.Println("received message:", string(msg.Body))
		fmt.Println("canceling context")
		cancel()

		// return error for nack + requeue
		return nil
	})

	_ = amqpx.Start(
		ctx,
		amqpx.NewURL("localhost", 5672, "admin", "password"), // or amqp://username@password:localhost:5672
		amqpx.WithLogger(logging.NewNoOpLogger()),            // provide a logger that implements the logging.Logger interface
	)
	defer amqpx.Close()

	_ = amqpx.Publish(ctx, "example-exchange", "route.name.v1.event", pool.Publishing{
		ContentType: "application/json",
		Body:        []byte("my test event"),
	})

	<-ctx.Done()
}

```


### Example with optional paramters

```go
package main

import (
	"context"
	"fmt"
	"os/signal"
	"syscall"

	"github.com/jxsl13/amqpx"
	"github.com/jxsl13/amqpx/logging"
	"github.com/jxsl13/amqpx/pool"
)

func SomeConsumer(cancel func()) pool.HandlerFunc {
	return func(ctx context.Context, msg pool.Delivery) error {
		fmt.Println("received message:", string(msg.Body))
		fmt.Println("canceling context")
		cancel()

		// return error for nack + requeue
		return nil
	}
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	amqpx.RegisterTopologyCreator(func(ctx context.Context, t *pool.Topologer) error {
		// error handling omitted for brevity

		_ = t.ExchangeDeclare(ctx, "example-exchange", "topic",
			pool.ExchangeDeclareOptions{
				Durable: true,
			},
		)
		_, _ = t.QueueDeclare(ctx, "example-queue",
			pool.QueueDeclareOptions{
				Durable: true,
				Args:    pool.QuorumQueue,
			},
		)
		t.QueueBind(ctx, "example-queue", "route.name.v1.event", "example-exchange")
		return nil
	})
	amqpx.RegisterTopologyDeleter(func(ctx context.Context, t *pool.Topologer) error {
		// error handling omitted for brevity
		_, _ = t.QueueDelete(ctx, "example-queue")
		_ = t.ExchangeDelete(ctx, "example-exchange")
		return nil
	})

	amqpx.RegisterHandler("example-queue",
		SomeConsumer(cancel),
		pool.ConsumeOptions{
			ConsumerTag: "example-queue-cunsumer",
			Exclusive:   true,
		},
	)

	_ = amqpx.Start(
		ctx,
		amqpx.NewURL("localhost", 5672, "admin", "password"), // or amqp://username@password:localhost:5672
		amqpx.WithLogger(logging.NewNoOpLogger()),            // provide a logger that implements the logging.Logger interface (logrus adapter is provided)
	)
	defer amqpx.Close()

	_ = amqpx.Publish(ctx, "example-exchange", "route.name.v1.event", pool.Publishing{
		ContentType: "application/json",
		Body:        []byte("my test event"),
	})

	<-ctx.Done()
}

```


## Types

The `amqpx` package provides a single type which incoorporates everything needed for consuming and publishing messages.

The `pool` package provides all of the implementation details .

### `amqpx.AMQPX`
The `AMQPX` struct consists at least one connection pool, a `Publisher`, a `Subscriber` and a `Topologer`.
Upon `Start(..)` and upon `Close()` a `Topologer` is created which creates the topology or destroys a topology based on one or *multiple* functions that were registered via `RegisterTopologyCreator` or `RegisterTopologyDeleter`.
After the topology has been created, a `Publisher` is instantiated from a publisher connection and session `Pool`.
The `Publisher` can be used to publish messages to specific *exchanges* with a given *routing key*.
In case you register an event handler function via `RegisterHandler` or `RegisterBatchHandler`, then another connection and session `Pool` is created which is then used to instantiate a `Subscriber`. The `Subscriber` communicates via one or multiple separate TCP connections in order to prevent interference between the `Publisher` and `Subscriber` (tcp pushback).

The `amqpx` package defines a global variable that allows the package `amqpx` to be used like the `AMQPX` object.

### `pool.Topologer`
The `Topologer` allows to create, delete, bind or unbind *exchanges* or *queues*

### `pool.Publisher`
The `Publisher` allows to publish individual events or messages to *exchanges* with a given *routing key*.

### `pool.Subscriber`
The `Subscriber` allows to register event handler functions that *consume messages from individual queues*.
A `Subscriber` must be `Start()`ed in order for it to create consumer goroutines that process events from broker queues.

## Development

Tests can all be run in parallel but the parallel testing is disabled for now because of the GitHub runners starting to behave weirdly when under such a load.
That is why those tests were disabled for the CI pipeline.

Test flags you might want to add:
```shell
go test -v -race -count=1 ./...
```
- see test logs
- detect data races
- do not cache test results

Starting the tests:
```shell
go test -v -race -count=1 ./...
```

### Test environment

- Requires docker (and docker compose subcommand)

Starting the test environment:
```shell
make environment
#or
docker compose up -d
```

The test environment looks like this:

Web interfaces:
 - username: `admin` and password: `password`
 - [rabbitmq management interface: http://127.0.0.1:15672 -> rabbitmq:15672](http://127.0.0.1:15672)
 - [out of memory rabbitmq management interface: http://127.0.0.1:25672 -> rabbitmq-broken:15672](http://127.0.0.1:25672)

```
127.0.0.1:5670 	-> rabbitmq-broken:5672 	# out of memory rabbitmq
127.0.0.1:5671 	-> rabbitmq:5672 			# healthy rabbitmq connection which is never disconnected


127.0.0.1:5672	-> toxiproxy:5672	-> rabbitmq:5672	# connection which is disconnected by toxiproxy
127.0.0.1:5673	-> toxiproxy:5673	-> rabbitmq:5672	# connection which is disconnected by toxiproxy
127.0.0.1:5674	-> toxiproxy:5674	-> rabbitmq:5672	# connection which is disconnected by toxiproxy
...
127.0.0.1:5771	-> toxiproxy:5771	-> rabbitmq:5672	# connection which is disconnected by toxiproxy

```
