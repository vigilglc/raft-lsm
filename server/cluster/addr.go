package cluster

import (
	"fmt"
)

type AddrInfo struct {
	Name        string `json:"name,omitempty"`
	Host        string `json:"host,omitempty"`
	ServicePort uint16 `json:"servicePort,omitempty"`
	RaftPort    uint16 `json:"raftPort,omitempty"`
}

func (addr *AddrInfo) ServiceAddress() string {
	return fmt.Sprintf("%s:%d", addr.Host, addr.ServicePort)
}

func (addr *AddrInfo) RaftAddress() string {
	return fmt.Sprintf("%s:%d", addr.Host, addr.RaftPort)
}
