package pool_test

import (
	"sync"
	"testing"
	"time"

	toxiproxy "github.com/Shopify/toxiproxy/v2/client"
	"github.com/jxsl13/amqpx/logging"
)

func NewProxy(t *testing.T) *toxiproxy.Proxy {
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

	return proxy
}

// https://github.com/Shopify/toxiproxy#toxics
func DisconnectWithStop(t *testing.T, block, timeout, duration time.Duration) (wait func()) {
	start := time.Now().Add(timeout)
	var wg sync.WaitGroup

	wg.Add(1)
	go func(start time.Time) {
		defer wg.Done()
		log := logging.NewTestLogger(t)
		proxy := NewProxy(t)

		time.Sleep(time.Until(start))
		log.Debug("disabled rabbitmq connection")
		err := proxy.Disable()
		if err != nil {
			log.Fatal(err)
		}

		time.Sleep(duration)
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
		wg.Wait()
	}
}

// https://github.com/Shopify/toxiproxy#toxics
func DisconnectWithStartStop(t *testing.T, block, timeout, duration time.Duration) (awaitStarted, awaitStopped func()) {
	start := time.Now().Add(timeout)
	var (
		wgStart sync.WaitGroup
		wgStop  sync.WaitGroup
	)

	wgStart.Add(1)
	wgStop.Add(1)
	go func(start time.Time) {
		defer wgStop.Done()
		log := logging.NewTestLogger(t)
		proxy := NewProxy(t)

		time.Sleep(time.Until(start))
		log.Debug("disabled rabbitmq connection")
		err := proxy.Disable()
		if err != nil {
			log.Fatal(err)
		}
		wgStart.Done()

		time.Sleep(duration)
		log.Debug("enabled rabbitmq connection")
		err = proxy.Enable()
		if err != nil {
			log.Fatal(err)
		}
	}(start)
	if block > 0 {
		time.Sleep(block)
	}

	return func() { wgStart.Wait() }, func() { wgStop.Wait() }
}
