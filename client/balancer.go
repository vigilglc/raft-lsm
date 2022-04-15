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
	Context() context.Context
	Close() error

	AddClient(client Client)
	AddClients(clients ...Client)
	AllClientHosts() []string

	Resolve() error
	Pick() (client Client, release func()) // TODO: use grpc balancer to do random pick, balancer.Register
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
	ret.hostClientM = map[string]*lockerClient{}
	ret.ctx, ret.cancel = context.WithCancel(ctx)
	return ret
}

func (blc *balancer) Context() context.Context {
	defer syncutil.SchedLockers(blc.rwmu.RLocker())()
	return blc.ctx
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

func (blc *balancer) AllClientHosts() []string {
	defer syncutil.SchedLockers(blc.rwmu.RLocker())()
	var hosts []string
	for _, cli := range blc.clients {
		hosts = append(hosts, cli.Host())
	}
	return hosts
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
	resolveTimeout    = 10 * time.Second
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
	blc.members = resp.Members
	blc.hostClientM = map[string]*lockerClient{}
	blc.clients = nil
	for _, mem := range resp.Members {
		host := GetMemberHost(mem)
		blc.doAddClient(NewClient(blc.ctx, host))
	}
	return err
}

func GetMemberHost(mem *rpcpb.Member) string {
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
			locked := blc.hostClientM[GetMemberHost(mem)]
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
			locked := blc.hostClientM[GetMemberHost(mem)]
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
			locked = blc.hostClientM[GetMemberHost(mem)]
			break
		}
	}
	defer syncutil.SchedLockers(locked)()
	host := GetMemberHost(member)
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
		if GetMemberHost(mem) == host {
			blc.members = append(blc.members[:i], blc.members[i+1:]...)
			locked = blc.hostClientM[GetMemberHost(mem)]
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
