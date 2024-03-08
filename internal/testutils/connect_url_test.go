package testutils

import (
	"encoding/json"
	"fmt"
	"testing"
)

/*
[

	{
	  "name": "rabbitmq-5673",
	  "listen": "[::]:5673",
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

	nextConnectURL := NewConnectURLGenerator(ExcludedPorts...)

	for i := 0; i < NumTests; i++ {
		proxyName, _, proxyPort := nextConnectURL()
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
	nextConnectURL := NewConnectURLGenerator(ExcludedPorts...)

	str := ""
	for i := 0; i < NumTests; i++ {
		proxyName, _, proxyPort := nextConnectURL()
		str += fmt.Sprintf("      - %[1]d:%[1]d # %s\n", proxyPort, proxyName)
	}
	fmt.Println(str)
}
