package proxyutils

import (
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	toxiproxy "github.com/Shopify/toxiproxy/v2/client"
	"github.com/jxsl13/amqpx/internal/testlogger"
	"github.com/stretchr/testify/require"
)

type Proxy struct {
	proxy           *toxiproxy.Proxy
	log             *slog.Logger
	lastHttpRequest time.Time
	closed          bool
	mu              sync.Mutex
}

func NewProxy(t *testing.T, proxyName string) *Proxy {
	log := testlogger.NewTestLogger(t)
	toxi := toxiproxy.NewClient("localhost:8474")

	m, err := toxi.Proxies()
	if err != nil {
		require.NoError(t, err)
	}

	var proxy *toxiproxy.Proxy
	proxy, found := m[proxyName]
	require.Truef(t, found, "no proxy with name %s found", proxyName)
	require.NotNil(t, proxy, "proxy with name %s is nil", proxyName)

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
		time.Sleep(time.Second)
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
		time.Sleep(time.Second)
	}
	return err
}

func (p *Proxy) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}

	// TODO: this can be removed
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
func DisconnectWithStopped(t *testing.T, proxyName string, block, timeout, duration time.Duration) (wait func()) {
	start := time.Now().Add(timeout)

	var (
		wg    sync.WaitGroup
		proxy = NewProxy(t, proxyName)
	)
	require.NoError(t, proxy.Enable())

	wg.Add(1)
	go func(start time.Time) {
		defer wg.Done()
		log := testlogger.NewTestLogger(t)

		if wait := time.Until(start); wait > 0 {
			time.Sleep(wait)
		}
		log.Debug("disabled rabbitmq connection")
		err := proxy.Disable()
		if err != nil {
			require.NoError(t, err)
		}

		if duration > 0 {
			time.Sleep(duration)
		}
		log.Debug(fmt.Sprintf("enabled rabbitmq connection proxy: %s", proxyName))
		err = proxy.Enable()
		if err != nil {
			require.NoError(t, err)
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
func DisconnectWithStartedStopped(t *testing.T, proxyName string, block, startIn, duration time.Duration) (awaitStarted, awaitStopped func()) {
	start := time.Now().Add(startIn)
	var (
		wgStart sync.WaitGroup
		wgStop  sync.WaitGroup
		proxy   = NewProxy(t, proxyName)
	)
	require.NoError(t, proxy.Enable())

	wgStart.Add(1)
	wgStop.Add(1)
	go func(start time.Time) {
		defer wgStop.Done()
		log := testlogger.NewTestLogger(t)

		if wait := time.Until(start); wait > 0 {
			time.Sleep(wait)
		}
		log.Debug(fmt.Sprintf("disabled rabbitmq connection proxy: %s", proxyName))
		err := proxy.Disable()
		if err != nil {
			require.NoError(t, err)
		}
		wgStart.Done()

		if duration > 0 {
			time.Sleep(duration)
		}
		log.Debug(fmt.Sprintf("enabled rabbitmq connection proxy: %s", proxyName))
		err = proxy.Enable()
		if err != nil {
			require.NoError(t, err)
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

func Disconnect(t *testing.T, proxyName string, duration time.Duration) (started, stopped func()) {
	var (
		disconnectOnce sync.Once
		reconnectOnce  sync.Once
	)

	disconnect, awaitStarted, awaitStopped := DisconnectWithStartStartedStopped(t, proxyName, duration)
	return func() {
			disconnectOnce.Do(func() {
				disconnect()
				awaitStarted()
			})
		}, func() {
			reconnectOnce.Do(awaitStopped)
		}
}

// block current thread
// timeout: how long the asynchronous goroutine needs to wait until it disables the connection
// duration: how long the asynchronous goroutine wait suntil it reenables the connection
func DisconnectWithStartStartedStopped(t *testing.T, proxyName string, duration time.Duration) (disconnect, awaitStarted, awaitStopped func()) {
	var (
		wgStart sync.WaitGroup
		wgStop  sync.WaitGroup
		proxy   = NewProxy(t, proxyName)
	)
	require.NoError(t, proxy.Enable())

	disconnect = func() {
		wgStart.Add(1)
		wgStop.Add(1)
		go func() {
			defer wgStop.Done()
			log := testlogger.NewTestLogger(t)

			log.Debug(fmt.Sprintf("disabled rabbitmq connection proxy: %s", proxyName))
			err := proxy.Disable()
			if err != nil {
				require.NoError(t, err)
			}
			wgStart.Done()

			if duration > 0 {
				time.Sleep(duration)
			}
			log.Debug(fmt.Sprintf("enabled rabbitmq connection proxy: %s", proxyName))
			err = proxy.Enable()
			if err != nil {
				require.NoError(t, err)
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
