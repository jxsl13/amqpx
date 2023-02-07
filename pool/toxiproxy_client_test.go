package pool_test

import (
	"sync"
	"testing"
	"time"

	toxiproxy "github.com/Shopify/toxiproxy/v2/client"
	"github.com/jxsl13/amqpx/logging"
)

type Proxy struct {
	proxy           *toxiproxy.Proxy
	log             logging.Logger
	lastHttpRequest time.Time
	closed          bool
	mu              sync.Mutex
}

func NewProxy(t *testing.T) *Proxy {
	log := logging.NewTestLogger(t)
	toxi := toxiproxy.NewClient("localhost:8474")

	m, err := toxi.Proxies()
	if err != nil {
		log.Fatal(err)
	}

	var proxy *toxiproxy.Proxy
	for k, p := range m {
		if k == "rabbitmq" {
			proxy = p
		}
	}
	if proxy == nil {
		log.Fatal("no rabbitmq proxy found")
	}

	return &Proxy{
		proxy:           proxy,
		log:             log,
		lastHttpRequest: time.UnixMilli(0),
	}
}

func (p *Proxy) Enable() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var err error
	for retries := 0; retries < 100; retries++ {
		p.lastHttpRequest = time.Now()
		err = p.proxy.Enable()
		if err == nil {
			return nil
		}
		if err != nil {
			time.Sleep(time.Second)
		}
	}
	return err
}

func (p *Proxy) Disable() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	var err error
	for retries := 0; retries < 100; retries++ {
		p.lastHttpRequest = time.Now()
		err = p.proxy.Disable()
		if err == nil {
			return nil
		}
		if err != nil {
			time.Sleep(time.Second)
		}
	}
	return err
}

func (p *Proxy) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}

	waitTime := time.Until(p.lastHttpRequest.Add(10 * time.Second))
	if waitTime > 0 {
		time.Sleep(waitTime)
	}
	p.closed = true
	return nil
}

// https://github.com/Shopify/toxiproxy#toxics
// block current thread
// timeout: how long the asynchronous goroutine needs to wait until it disables the connection
// duration: how long the asynchronous goroutine wait suntil it reenables the connection
func DisconnectWithStopped(t *testing.T, block, timeout, duration time.Duration) (wait func()) {
	start := time.Now().Add(timeout)

	var (
		wg    sync.WaitGroup
		proxy = NewProxy(t)
	)
	wg.Add(1)
	go func(start time.Time) {
		defer wg.Done()
		log := logging.NewTestLogger(t)

		if wait := time.Until(start); wait > 0 {
			time.Sleep(wait)
		}
		log.Debug("disabled rabbitmq connection")
		err := proxy.Disable()
		if err != nil {
			log.Fatal(err)
		}

		if duration > 0 {
			time.Sleep(duration)
		}
		log.Debug("enabled rabbitmq connection")
		err = proxy.Enable()
		if err != nil {
			log.Fatal(err)
		}
	}(start)
	if block > 0 {
		time.Sleep(block)
	}

	return func() {
		proxy.Close()
		wg.Wait()
	}
}

// https://github.com/Shopify/toxiproxy#toxics
// block current thread
// timeout: how long the asynchronous goroutine needs to wait until it disables the connection
// duration: how long the asynchronous goroutine wait suntil it reenables the connection
func DisconnectWithStartedStopped(t *testing.T, block, timeout, duration time.Duration) (awaitStarted, awaitStopped func()) {
	start := time.Now().Add(timeout)
	var (
		wgStart sync.WaitGroup
		wgStop  sync.WaitGroup
		proxy   = NewProxy(t)
	)

	wgStart.Add(1)
	wgStop.Add(1)
	go func(start time.Time) {
		defer wgStop.Done()
		log := logging.NewTestLogger(t)

		if wait := time.Until(start); wait > 0 {
			time.Sleep(wait)
		}
		log.Debug("disabled rabbitmq connection")
		err := proxy.Disable()
		if err != nil {
			log.Fatal(err)
		}
		wgStart.Done()

		if duration > 0 {
			time.Sleep(duration)
		}
		log.Debug("enabled rabbitmq connection")
		err = proxy.Enable()
		if err != nil {
			log.Fatal(err)
		}
	}(start)
	if block > 0 {
		time.Sleep(block)
	}

	return func() {
			wgStart.Wait()
		}, func() {
			proxy.Close()
			wgStop.Wait()
		}
}

// block current thread
// timeout: how long the asynchronous goroutine needs to wait until it disables the connection
// duration: how long the asynchronous goroutine wait suntil it reenables the connection
func DisconnectWithStartStartedStopped(t *testing.T, duration time.Duration) (disconnect, awaitStarted, awaitStopped func()) {
	var (
		wgStart sync.WaitGroup
		wgStop  sync.WaitGroup
		proxy   = NewProxy(t)
	)
	disconnect = func() {
		wgStart.Add(1)
		wgStop.Add(1)
		go func() {
			defer wgStop.Done()
			log := logging.NewTestLogger(t)

			log.Debug("disabled rabbitmq connection")
			err := proxy.Disable()
			if err != nil {
				log.Fatal(err)
			}
			wgStart.Done()

			if duration > 0 {
				time.Sleep(duration)
			}
			log.Debug("enabled rabbitmq connection")
			err = proxy.Enable()
			if err != nil {
				log.Fatal(err)
			}
		}()
	}

	awaitStarted = func() {
		wgStart.Wait()
	}

	awaitStopped = func() {
		proxy.Close()
		wgStop.Wait()
	}

	return disconnect, awaitStarted, awaitStopped
}
