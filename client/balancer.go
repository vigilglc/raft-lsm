package client

import (
	"context"
	"github.com/vigilglc/raft-lsm/server/api/rpcpb"
	"github.com/vigilglc/raft-lsm/server/cluster"
	"github.com/vigilglc/raft-lsm/server/utils/syncutil"
	"math/rand"
	"sync"
	"time"
)

type Balancer interface {
	AddClient(client Client)
	AddClients(clients ...Client)
	Close() error

	Resolve() error
	Pick() (client Client, release func())
	PickByID(ID uint64) (client Client, release func())
	PickByName(name string) (client Client, release func())
	PickByHost(host string) (client Client, release func())

	RemoveByID(ID uint64)
	RemoveByHost(host string)
}

type balancer struct {
	ctx         context.Context
	cancel      func()
	rwmu        sync.RWMutex
	members     []*rpcpb.Member
	hostClientM map[string]*lockerClient
	clients     []*lockerClient
}

type lockerClient struct {
	sync.Mutex
	Client
}

func NewBalancer(ctx context.Context) Balancer {
	ret := new(balancer)
	ret.ctx, ret.cancel = context.WithCancel(ctx)
	return ret
}

func (blc *balancer) AddClient(client Client) {
	defer syncutil.SchedLockers(&blc.rwmu)()
	blc.doAddClient(client)
}

func (blc *balancer) doAddClient(client Client) {
	if _, ok := blc.hostClientM[client.Host()]; ok {
		return
	}
	lc := &lockerClient{Client: client}
	blc.hostClientM[client.Host()] = lc
	blc.clients = append(blc.clients, lc)
}

func (blc *balancer) AddClients(clients ...Client) {
	defer syncutil.SchedLockers(&blc.rwmu)()
	for _, cli := range clients {
		blc.doAddClient(cli)
	}
}

func (blc *balancer) Close() error {
	defer syncutil.SchedLockers(&blc.rwmu)()
	blc.cancel()
	var err error
	for _, cli := range blc.clients {
		if err == nil {
			err = cli.Close()
		}
	}
	return err
}

const (
	resolveRetryTimes = 5
	resolveTimeout    = 1000 * time.Millisecond
)

func (blc *balancer) Resolve() error {
	var resp *rpcpb.ClusterStatusResponse
	var err error
	for i := 0; i < resolveRetryTimes; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), resolveTimeout)
		client, release := blc.Pick()
		resp, err = client.ClusterStatus(ctx, &rpcpb.ClusterStatusRequest{Linearizable: true})
		release()
		cancel()
		if err == nil && len(resp.Members) > 0 {
			break
		}
	}
	if err != nil {
		return err
	}
	defer syncutil.SchedLockers(&blc.rwmu)()
	var client Client
	blc.members = resp.Members
	for _, mem := range resp.Members {
		host := getMemberHost(mem)
		client, err = NewClient(blc.ctx, host)
		if err == nil {
			blc.doAddClient(client)
		}
	}
	return err
}

func getMemberHost(mem *rpcpb.Member) string {
	addrInfo := cluster.AddrInfo{Host: mem.Host, ServicePort: uint16(mem.ServicePort), RaftPort: uint16(mem.RaftPort)}
	return addrInfo.ServiceAddress()
}

var rang = rand.New(rand.NewSource(time.Now().UnixNano()))

func (blc *balancer) Pick() (client Client, release func()) {
	defer syncutil.SchedLockers(blc.rwmu.RLocker())()
	locked := blc.clients[rang.Int()%len(blc.clients)]
	locked.Lock()
	if locked.Client.Closed() {
		_ = locked.Client.Reset()
	}
	return locked.Client, func() {
		locked.Unlock()
	}
}

func (blc *balancer) PickByID(ID uint64) (client Client, release func()) {
	defer syncutil.SchedLockers(blc.rwmu.RLocker())()
	for _, mem := range blc.members {
		if mem.ID == ID {
			locked := blc.hostClientM[getMemberHost(mem)]
			locked.Lock()
			if locked.Client.Closed() {
				_ = locked.Client.Reset()
			}
			return locked.Client, func() {
				locked.Unlock()
			}
		}
	}
	return nil, func() {}
}

func (blc *balancer) PickByName(name string) (client Client, release func()) {
	defer syncutil.SchedLockers(blc.rwmu.RLocker())()
	for _, mem := range blc.members {
		if mem.Name == name {
			locked := blc.hostClientM[getMemberHost(mem)]
			locked.Lock()
			if locked.Client.Closed() {
				_ = locked.Client.Reset()
			}
			return locked.Client, func() {
				locked.Unlock()
			}
		}
	}
	return nil, func() {}
}

func (blc *balancer) PickByHost(host string) (client Client, release func()) {
	defer syncutil.SchedLockers(blc.rwmu.RLocker())()
	for _, locked := range blc.clients {
		if locked.Host() == host {
			locked.Lock()
			if locked.Client.Closed() {
				_ = locked.Client.Reset()
			}
			return locked.Client, func() {
				locked.Unlock()
			}
		}
	}
	return nil, func() {}
}

func (blc *balancer) RemoveByID(ID uint64) {
	defer syncutil.SchedLockers(&blc.rwmu)()
	var locked *lockerClient
	var member *rpcpb.Member
	for i, mem := range blc.members {
		if mem.ID == ID {
			member = mem
			blc.members = append(blc.members[:i], blc.members[i+1:]...)
			locked = blc.hostClientM[getMemberHost(mem)]
			break
		}
	}
	defer syncutil.SchedLockers(locked)()
	host := getMemberHost(member)
	delete(blc.hostClientM, host)
	for i, lc := range blc.clients {
		if lc.Host() == host {
			blc.clients = append(blc.clients[:i], blc.clients[i+1:]...)
		}
		return
	}
}

func (blc *balancer) RemoveByHost(host string) {
	defer syncutil.SchedLockers(&blc.rwmu)()
	var locked *lockerClient
	for i, mem := range blc.members {
		if getMemberHost(mem) == host {
			blc.members = append(blc.members[:i], blc.members[i+1:]...)
			locked = blc.hostClientM[getMemberHost(mem)]
			break
		}
	}
	defer syncutil.SchedLockers(locked)()
	delete(blc.hostClientM, host)
	for i, lc := range blc.clients {
		if lc.Host() == host {
			blc.clients = append(blc.clients[:i], blc.clients[i+1:]...)
		}
		return
	}
}
