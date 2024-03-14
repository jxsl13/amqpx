package testutils

import (
	"fmt"
	"strings"
	"sync"
)

type generatorOptions struct {
	prefix       string
	up           int
	suffix       string
	randomSuffix bool
	suffixSize   int
}

func (o *generatorOptions) ToSuffix() string {
	var suffix string
	suffix += o.suffix
	if o.randomSuffix {
		suffix += "-" + RandString(o.suffixSize)
	}
	return suffix
}

type GeneratorOption func(*generatorOptions)

// WithUp sets the number of stack frames to skip when generating the name
func WithUp(up int) GeneratorOption {
	return func(o *generatorOptions) {
		o.up = up
	}
}

func WithRandomSuffix(addRandomSuffix bool) GeneratorOption {
	return func(o *generatorOptions) {
		o.randomSuffix = addRandomSuffix
	}
}

func WithPrefix(prefix string) GeneratorOption {
	return func(o *generatorOptions) {
		o.prefix = prefix
	}
}

func WithSuffix(suffix string) GeneratorOption {
	return func(o *generatorOptions) {
		o.suffix = suffix
	}
}

func RoutingKeyGenerator(sessionName string, options ...GeneratorOption) (nextRoutingKey func() string) {
	opts := generatorOptions{
		prefix:       "",
		randomSuffix: false,

		up: 2,
	}

	for _, opt := range options {
		opt(&opts)
	}

	var mu sync.Mutex
	var counter int64
	return func() string {
		mu.Lock()
		cnt := counter
		counter++
		mu.Unlock()
		return fmt.Sprintf("%s-%srouting-key-%d%s", sessionName, opts.prefix, cnt, opts.ToSuffix())
	}
}

func ExchangeNameGenerator(sessionName string, options ...GeneratorOption) (nextExchangeName func() string) {
	opts := generatorOptions{
		prefix:       "",
		randomSuffix: false,

		up: 2,
	}

	for _, opt := range options {
		opt(&opts)
	}

	var mu sync.Mutex
	var counter int64
	return func() string {
		mu.Lock()
		cnt := counter
		counter++
		mu.Unlock()
		return fmt.Sprintf("%s-%sexchange-%d%s", sessionName, opts.prefix, cnt, opts.ToSuffix())
	}
}

func ConsumerNameGenerator(queueName string, options ...GeneratorOption) (nextConsumerName func() string) {
	opts := generatorOptions{
		prefix:       "",
		randomSuffix: false,

		up: 2,
	}

	for _, opt := range options {
		opt(&opts)
	}

	var mu sync.Mutex
	var counter int64
	return func() string {
		mu.Lock()
		cnt := counter
		counter++
		mu.Unlock()
		return fmt.Sprintf("%s-%sconsumer-%d%s", queueName, opts.prefix, cnt, opts.ToSuffix())
	}
}

func QueueNameGenerator(sessionName string, options ...GeneratorOption) (nextQueueName func() string) {
	opts := generatorOptions{
		prefix:       "",
		randomSuffix: false,

		up: 2,
	}

	for _, opt := range options {
		opt(&opts)
	}

	var (
		mu      sync.Mutex
		counter int64
	)
	return func() string {
		mu.Lock()
		cnt := counter
		counter++
		mu.Unlock()
		return fmt.Sprintf("%s-%squeue-%d%s", sessionName, opts.prefix, cnt, opts.ToSuffix())
	}
}

func SessionNameGenerator(connectionName string, options ...GeneratorOption) (nextSessionName func() string) {
	opts := generatorOptions{
		prefix:       "",
		randomSuffix: false,

		up: 2,
	}

	for _, opt := range options {
		opt(&opts)
	}

	var (
		mu      sync.Mutex
		counter int64
	)
	return func() string {
		mu.Lock()
		cnt := counter
		counter++
		mu.Unlock()
		return fmt.Sprintf("%s-%ssession-%d%s", connectionName, opts.prefix, cnt, opts.ToSuffix())
	}
}

func PoolNameGenerator(options ...GeneratorOption) (nextConnName func() string) {
	opts := generatorOptions{
		prefix:       "",
		randomSuffix: false,

		up: 2,
	}

	for _, opt := range options {
		opt(&opts)
	}

	var mu sync.Mutex
	funcName := FuncName(opts.up)
	parts := strings.Split(funcName, ".")
	funcName = parts[len(parts)-1]

	var counter int64
	return func() string {
		mu.Lock()
		cnt := counter
		counter++
		mu.Unlock()
		return fmt.Sprintf("%s%s-%d%s", opts.prefix, funcName, cnt, opts.ToSuffix())
	}
}

func ConnectionNameGenerator(options ...GeneratorOption) (nextConnName func() string) {
	opts := generatorOptions{
		prefix:       "",
		randomSuffix: false,

		up: 2,
	}

	for _, opt := range options {
		opt(&opts)
	}

	var mu sync.Mutex
	funcName := FuncName(opts.up)
	parts := strings.Split(funcName, ".")
	funcName = parts[len(parts)-1]

	var counter int64
	return func() string {
		mu.Lock()
		cnt := counter
		counter++
		mu.Unlock()
		return fmt.Sprintf("%s%s-%d%s", opts.prefix, funcName, cnt, opts.ToSuffix())
	}
}

func MessageGenerator(queueOrExchangeName string, options ...GeneratorOption) (nextMessage func() string) {
	opts := generatorOptions{
		prefix:       "",
		randomSuffix: false,
		up:           2,
	}

	for _, opt := range options {
		opt(&opts)
	}

	var mu sync.Mutex
	var counter int64
	return func() string {
		mu.Lock()
		cnt := counter
		counter++
		mu.Unlock()
		return fmt.Sprintf("%s-message-%d%s", queueOrExchangeName, cnt, opts.ToSuffix())
	}
}

func NewExchangeQueueGenerator(funcName string) func() ExchangeQueue {
	var (
		nextExchangeName = ExchangeNameGenerator(funcName)
		nextQueueName    = QueueNameGenerator(funcName)
		nextRoutingKey   = RoutingKeyGenerator(funcName)
	)
	return func() ExchangeQueue {
		return NewExchangeQueue(nextExchangeName(), nextQueueName(), nextRoutingKey())
	}
}

func NewExchangeQueue(exchange, queue, routingKey string) ExchangeQueue {
	return ExchangeQueue{
		Exchange:    exchange,
		Queue:       queue,
		RoutingKey:  routingKey,
		ConsumerTag: ConsumerNameGenerator(queue)(), // generate one consumer name
		NextPubMsg:  MessageGenerator(exchange),
		NextSubMsg:  MessageGenerator(exchange),
	}
}

type ExchangeQueue struct {
	Exchange   string
	Queue      string
	RoutingKey string

	ConsumerTag string

	NextPubMsg func() string
	NextSubMsg func() string
}
