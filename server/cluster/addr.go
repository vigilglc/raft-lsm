package cluster

import (
	"fmt"
)

type AddrInfo struct {
	Name        string `json:"name,omitempty"`
	Host        string `json:"host"`
	ServicePort uint16 `json:"servicePort"`
	RaftPort    uint16 `json:"raftPort"`
}

func (addr *AddrInfo) ServiceAddress() string {
	return fmt.Sprintf("%s:%d", addr.Host, addr.ServicePort)
}

func (addr *AddrInfo) RaftAddress() string {
	return fmt.Sprintf("%s:%d", addr.Host, addr.RaftPort)
}
