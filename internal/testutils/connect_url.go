package testutils

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"
)

var (
	NumTests          = 100
	Upstream          = "rabbitmq:5672"
	Username          = "admin"
	Password          = "password"
	Hostname          = "localhost"
	BrokenConnectURL  = fmt.Sprintf("amqp://%s:%s@%s:%d/", Username, Password, Hostname, 5670)
	HealthyConnectURL = fmt.Sprintf("amqp://%s:%s@%s:%d/", Username, Password, Hostname, 5671)
)

var (
	port int = 5672
	mu   sync.Mutex
)

func NextConnectURL() (proxyName, connectURL string, port int) {
	proxyPort := NextPort()
	return fmt.Sprintf("rabbitmq-%d", proxyPort),
		fmt.Sprintf("amqp://%s:%s@%s:%d/", Username, Password, Hostname, proxyPort),
		proxyPort
}

func NextPort() int {
	mu.Lock()
	defer mu.Unlock()
	defer func() {
	loop:
		for {
			port++
			switch port {
			case 5670, 5671, 8474:
			default:
				break loop
			}

		}
	}()
	return port
}

/*
[

	{
	  "name": "rabbitmq",
	  "listen": "[::]:5672",
	  "upstream": "rabbitmq:5672",
	  "enabled": true
	}

]
*/
func TestGenerateProxyConfig(_ *testing.T) {
	list := []struct {
		Name     string `json:"name"`
		Listen   string `json:"listen"`
		Upstream string `json:"upstream"`
		Enabled  bool   `json:"enabled"`
	}{}

	for i := 0; i < NumTests; i++ {
		_, proxyName, proxyPort := NextConnectURL()
		list = append(list, struct {
			Name     string `json:"name"`
			Listen   string `json:"listen"`
			Upstream string `json:"upstream"`
			Enabled  bool   `json:"enabled"`
		}{
			Name:     proxyName,
			Listen:   fmt.Sprintf("[::]:%d", proxyPort),
			Upstream: Upstream,
			Enabled:  true,
		})
	}

	data, _ := json.MarshalIndent(list, "", "  ")
	fmt.Println(string(data))
}

func TestGenerateDockerPortForwards(_ *testing.T) {
	str := ""
	for i := 0; i < NumTests; i++ {
		proxyName, _, proxyPort := NextConnectURL()
		str += fmt.Sprintf("      - %[1]d:%[1]d # %s\n", proxyPort, proxyName)
	}
	fmt.Println(str)
}
