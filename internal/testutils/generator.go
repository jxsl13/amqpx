package testutils

import (
	"fmt"
	"strings"
	"sync"
	"testing"
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

func PoolNameGenerator(funcName string, options ...GeneratorOption) (nextConnName func() string) {
	opts := generatorOptions{
		prefix:       "",
		randomSuffix: false,

		up: 2,
	}

	for _, opt := range options {
		opt(&opts)
	}

	var mu sync.Mutex
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
		mu               sync.Mutex
		nextExchangeName = ExchangeNameGenerator(funcName)
		nextQueueName    = QueueNameGenerator(funcName)
		nextRoutingKey   = RoutingKeyGenerator(funcName)
	)
	return func() ExchangeQueue {
		mu.Lock()
		defer mu.Unlock()
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

	lastAssertedSubMsg string
	assertionStarted   bool
}

func (eq *ExchangeQueue) ValidateNextSubMsg(t *testing.T, msg string) error {
	t.Helper()

	if !eq.assertionStarted {
		eq.assertionStarted = true
		eq.lastAssertedSubMsg = eq.NextSubMsg()

		if eq.lastAssertedSubMsg != msg {
			return fmt.Errorf("expected message %q, got %q", eq.lastAssertedSubMsg, msg)
		}
		return nil
	}

	// the new message is either the previous message due to connection problems and re-delivery
	// or a new message

	if eq.lastAssertedSubMsg == msg {
		return nil
	}

	eq.lastAssertedSubMsg = eq.NextSubMsg()

	if eq.lastAssertedSubMsg != msg {
		return fmt.Errorf("expected message %q, got %q", eq.lastAssertedSubMsg, msg)
	}
	return nil

}
