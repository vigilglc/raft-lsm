package cluster

import (
	"fmt"
)

type AddrInfo struct {
	Name        string `json:"name,omitempty"  toml:"Name"`
	Host        string `json:"host" toml:"Host"`
	ServicePort uint16 `json:"servicePort" toml:"ServicePort"`
	RaftPort    uint16 `json:"raftPort" toml:"RaftPort"`
}

func (addr *AddrInfo) ServiceAddress() string {
	return fmt.Sprintf("%s:%d", addr.Host, addr.ServicePort)
}

func (addr *AddrInfo) RaftAddress() string {
	return fmt.Sprintf("%s:%d", addr.Host, addr.RaftPort)
}

func AddInfoEmpty(info AddrInfo) bool {
	return len(info.Name) == 0 || len(info.Host) == 0
}
