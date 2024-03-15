package testutils

import (
	"fmt"
	"sync"
)

var (
	NumTests          = 100
	Upstream          = "rabbitmq:5672"
	Username          = "admin"
	Password          = "password"
	Hostname          = "localhost"
	ToxiProxyPort     = 8474
	BrokenPort        = 5670
	HealthyPort       = 5671
	ExcludedPorts     = []int{BrokenPort, HealthyPort, ToxiProxyPort}
	BrokenConnectURL  = fmt.Sprintf("amqp://%s:%s@%s:%d/", Username, Password, Hostname, BrokenPort)
	HealthyConnectURL = fmt.Sprintf("amqp://%s:%s@%s:%d/", Username, Password, Hostname, HealthyPort)

	nextPort = NewPortGenerator(ExcludedPorts...)
)

func NextConnectURL() (proxyName, connectURL string, port int) {
	proxyPort := NextPort()
	return fmt.Sprintf("rabbitmq-%d", proxyPort),
		fmt.Sprintf("amqp://%s:%s@%s:%d/", Username, Password, Hostname, proxyPort),
		proxyPort
}

func NextPort() int {
	return nextPort()
}

func NewConnectURLGenerator(excludePorts ...int) func() (proxyName, connectURL string, port int) {
	portGen := NewPortGenerator(excludePorts...)
	return func() (proxyName, connectURL string, port int) {
		proxyPort := portGen()
		return fmt.Sprintf("rabbitmq-%d", proxyPort),
			fmt.Sprintf("amqp://%s:%s@%s:%d/", Username, Password, Hostname, proxyPort),
			proxyPort
	}
}

func NewPortGenerator(excludePorts ...int) func() int {
	var (
		port int = 5672
		mu   sync.Mutex
	)
	excludeMap := make(map[int]struct{}, len(excludePorts))
	for _, p := range excludePorts {
		excludeMap[p] = struct{}{}
	}
	return func() int {
		mu.Lock()
		defer mu.Unlock()
		defer func() {
		loop:
			for {
				port++

				if _, ok := excludeMap[port]; ok {
					continue loop
				}
				break loop

			}
		}()
		return port
	}

}
