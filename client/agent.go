package client

import (
	"context"
	"errors"
	"github.com/vigilglc/raft-lsm/server/api/rpcpb"
	"github.com/vigilglc/raft-lsm/server/cluster"
	"github.com/vigilglc/raft-lsm/server/utils/ntfyutil"
	"github.com/vigilglc/raft-lsm/server/utils/syncutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"math/rand"
	"strings"
	"sync"
	"time"
)

var (
	ErrNoAvailableClients = errors.New("client-agent: no available clients")
	ErrAgentStopped       = errors.New("client-agent: stopped")
)

type Agent interface {
	Context() context.Context
	AllIDs() []uint64
	AllHosts() []string
	AddClients(hosts []string)
	Close() error
	Resolve() error
	Pick(act func(context.Context, Client) error) error
	PickByID(ID uint64, act func(context.Context, Client) error) error
}

type agent struct {
	ctx          context.Context
	cancel       func()
	rwmu         sync.RWMutex
	rng          *rand.Rand
	aliveClients map[string]*rpcClient
	deadClients  map[string]*rpcClient

	aliveClientsSlc []*rpcClient

	stopped chan struct{}
	done    chan struct{}

	emitter    *ntfyutil.SharedVEmitter
	resolveReq chan struct{}
}

func NewAgent(ctx context.Context, initHosts ...string) Agent {
	ret := new(agent)
	ret.ctx, ret.cancel = context.WithCancel(ctx)
	ret.rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	ret.doClean()
	ret.stopped = make(chan struct{})
	ret.done = make(chan struct{})

	ret.emitter = ntfyutil.NewSharedVEmitter()
	ret.resolveReq = make(chan struct{})

	ret.AddClients(initHosts)
	go func() {
		var timer = time.NewTimer(resolveInterval)
		for true {
			select {
			case <-ret.stopped:
				close(ret.done)
				return
			case <-timer.C:
				_ = ret.doResolve()
				timer.Reset(resolveInterval)
			case <-ret.resolveReq:
				err := ret.doResolve()
				ret.emitter.NextShared().Notify(err)
				timer.Reset(resolveInterval)
			}
		}
	}()
	return ret
}

func (agt *agent) Context() context.Context {
	return agt.ctx
}

func (agt *agent) AddClients(hosts []string) {
	defer syncutil.SchedLockers(&agt.rwmu)()
	for _, host := range hosts {
		agt.doAddClient(host)
	}
}

func (agt *agent) doAddClient(host string) {
	host = strings.TrimSpace(host)
	if _, ok := agt.aliveClients[host]; ok {
		return
	}
	if cli, ok := agt.deadClients[host]; ok {
		err := cli.doConnect()
		if err != nil {
			return
		}
	}
	if cli, err := newClient(agt.ctx, agt, host); err == nil {
		agt.aliveClients[host] = cli
		agt.aliveClientsSlc = append(agt.aliveClientsSlc, cli)
	} else {
		agt.deadClients[host] = cli
	}
}

func (agt *agent) doClean() {
	agt.aliveClients = map[string]*rpcClient{}
	agt.deadClients = map[string]*rpcClient{}
	agt.aliveClientsSlc = nil
}

func (agt *agent) AllHosts() []string {
	defer syncutil.SchedLockers(agt.rwmu.RLocker())()
	var hosts []string
	for _, hc := range [...]map[string]*rpcClient{agt.aliveClients, agt.deadClients} {
		for _, cli := range hc {
			hosts = append(hosts, cli.Host())
		}
	}
	return hosts
}

func (agt *agent) AllIDs() []uint64 {
	defer syncutil.SchedLockers(agt.rwmu.RLocker())()
	var ids []uint64
	for _, hc := range [...]map[string]*rpcClient{agt.aliveClients, agt.deadClients} {
		for _, cli := range hc {
			ids = append(ids, cli.id)
		}
	}
	return ids
}

func (agt *agent) Close() error {
	defer syncutil.SchedLockers(&agt.rwmu)()
	agt.cancel()
	var err error
	for _, hc := range [...]map[string]*rpcClient{agt.aliveClients} {
		for _, cli := range hc {
			er := cli.doClose()
			if err == nil {
				err = er
			}
		}
	}
	agt.doClean()
	close(agt.stopped)
	<-agt.done
	return err
}

const (
	resolveRetryTimes = 5
	resolveTimeout    = 1 * time.Second
	resolveInterval   = 10 * time.Second
)

func (agt *agent) Resolve() error {
	sharedV := agt.emitter.CurrentShared(func() {
		agt.resolveReq <- struct{}{}
	})
	select {
	case <-agt.stopped:
		return ErrAgentStopped
	case <-sharedV.Wait():
	}
	v, ok := sharedV.Value().(error)
	if ok {
		return v
	}
	return nil
}

func (agt *agent) doResolve() error {
	var resp *rpcpb.ClusterStatusResponse
	var err error
	var act = func(ctx context.Context, client Client) error {
		ctx, cancel := context.WithTimeout(ctx, resolveTimeout)
		defer cancel()
		resp, err = client.ClusterStatus(ctx, &rpcpb.ClusterStatusRequest{Linearizable: true})
		return err
	}
	var resolved = func() bool {
		return err == nil && resp != nil && len(resp.Members) > 0
	}
	for i := 0; i < resolveRetryTimes; i++ {
		_ = agt.Pick(act)
		if resolved() {
			break
		}
	}
	if !resolved() {
		for _, ID := range agt.deadIDs() {
			_ = agt.PickByID(ID, act)
			if resolved() {
				break
			}
		}
	}
	if !resolved() {
		return err
	}
	hosts := map[string]struct{}{}
	defer syncutil.SchedLockers(&agt.rwmu)()
	for _, mem := range resp.Members {
		host := GetMemberHost(mem)
		hosts[host] = struct{}{}
		agt.doAddClient(host)
	}
	agt.aliveClientsSlc = agt.aliveClientsSlc[:0]
	for h, cli := range agt.aliveClients {
		if _, ok := hosts[h]; !ok {
			_ = cli.doClose()
			delete(agt.aliveClients, h)
		} else {
			agt.aliveClientsSlc = append(agt.aliveClientsSlc, cli)
		}
	}
	for h := range agt.deadClients {
		if _, ok := hosts[h]; !ok {
			delete(agt.deadClients, h)
		}
	}
	return err
}

func (agt *agent) Pick(act func(context.Context, Client) error) error {
	for {
		agt.rwmu.RLock()
		if len(agt.aliveClientsSlc) == 0 {
			return ErrNoAvailableClients
		}
		cli := agt.aliveClientsSlc[agt.rng.Int()%len(agt.aliveClientsSlc)]
		err := act(agt.ctx, cli)
		agt.rwmu.RUnlock()
		if shouldCloseClient(err) {
			agt.aliveClient2Dead(cli.host)
		} else {
			return err
		}
	}
}

func (agt *agent) PickByID(ID uint64, act func(context.Context, Client) error) error {
	agt.rwmu.RLock()
	if len(agt.aliveClients)+len(agt.deadClients) == 0 {
		return ErrNoAvailableClients
	}
	var err error
	var client *rpcClient
	for _, hc := range [...]map[string]*rpcClient{agt.aliveClients, agt.deadClients} {
		for _, cli := range hc {
			if cli.id == ID {
				client = cli
				break
			}
		}
	}
	if client == nil {
		agt.rwmu.RUnlock()
		return ErrNoAvailableClients
	}
	var revoked bool
	if err = client.doConnect(); err != nil {
		agt.rwmu.RUnlock()
		return err
	} else {
		revoked = true
	}
	err = act(agt.ctx, client)
	agt.rwmu.RUnlock()
	if shouldCloseClient(err) {
		agt.aliveClient2Dead(client.host)
	} else if revoked {
		agt.deadClient2Alive(client.host)
	}
	return err
}

func shouldCloseClient(err error) bool {
	if err == nil {
		return false
	}
	if err == grpc.ErrServerStopped {
		return true
	}
	if s, ok := status.FromError(err); ok {
		if s == nil {
			return false
		}
		switch s.Code() {
		case codes.Internal:
			return true
		case codes.Unavailable:
			return true
		}
	}
	return false
}

func (agt *agent) aliveClient2Dead(host string) {
	defer syncutil.SchedLockers(&agt.rwmu)()
	if cli, ok := agt.aliveClients[host]; ok {
		agt.deadClients[host] = cli
		agt.aliveClientsSlc = agt.aliveClientsSlc[:0]
		for _, cli := range agt.aliveClients {
			agt.aliveClientsSlc = append(agt.aliveClientsSlc, cli)
		}
	}
}

func (agt *agent) deadClient2Alive(host string) {
	defer syncutil.SchedLockers(&agt.rwmu)()
	if cli, ok := agt.deadClients[host]; ok {
		agt.aliveClients[host] = cli
	}
}

func (agt *agent) deadIDs() []uint64 {
	agt.rwmu.RLock()
	var deadIDs []uint64
	for _, cli := range agt.deadClients {
		deadIDs = append(deadIDs, cli.ID())
	}
	agt.rwmu.RUnlock()
	return deadIDs
}

func GetMemberHost(mem *rpcpb.Member) string {
	addrInfo := cluster.AddrInfo{Host: mem.Host, ServicePort: uint16(mem.ServicePort), RaftPort: uint16(mem.RaftPort)}
	return addrInfo.ServiceAddress()
}
