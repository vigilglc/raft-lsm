package client

import (
	"context"
	"github.com/vigilglc/raft-lsm/server/api/rpcpb"
	"github.com/vigilglc/raft-lsm/server/utils/syncutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"sync"
)

type Client interface {
	rpcpb.KVServiceClient
	rpcpb.ClusterServiceClient
	rpcpb.RaftServiceClient
	sync.Locker
	ID() uint64
	Host() string
}

type rpcClient struct {
	ctx context.Context
	agt *agent

	sync.RWMutex
	host       string
	id         uint64
	closed     bool
	conn       *grpc.ClientConn
	kvCli      rpcpb.KVServiceClient
	clusterCli rpcpb.ClusterServiceClient
	raftCli    rpcpb.RaftServiceClient
}

func (rc *rpcClient) Get(ctx context.Context, in *rpcpb.GetRequest, opts ...grpc.CallOption) (*rpcpb.GetResponse, error) {
	defer syncutil.SchedLockers(rc.RLocker())()
	return rc.kvCli.Get(ctx, in, opts...)
}

func (rc *rpcClient) Put(ctx context.Context, in *rpcpb.PutRequest, opts ...grpc.CallOption) (*rpcpb.PutResponse, error) {
	defer syncutil.SchedLockers(rc.RLocker())()
	return rc.kvCli.Put(ctx, in, opts...)
}

func (rc *rpcClient) Del(ctx context.Context, in *rpcpb.DelRequest, opts ...grpc.CallOption) (*rpcpb.DelResponse, error) {
	defer syncutil.SchedLockers(rc.RLocker())()
	return rc.kvCli.Del(ctx, in, opts...)
}

func (rc *rpcClient) Write(ctx context.Context, in *rpcpb.WriteRequest, opts ...grpc.CallOption) (*rpcpb.WriteResponse, error) {
	defer syncutil.SchedLockers(rc.RLocker())()
	return rc.kvCli.Write(ctx, in, opts...)
}

func (rc *rpcClient) Range(ctx context.Context, opts ...grpc.CallOption) (rpcpb.KVService_RangeClient, error) {
	defer syncutil.SchedLockers(rc.RLocker())()
	return rc.kvCli.Range(ctx, opts...)
}

func (rc *rpcClient) LocalMember(ctx context.Context, in *rpcpb.LocalMemberRequest, opts ...grpc.CallOption) (*rpcpb.LocalMemberResponse, error) {
	defer syncutil.SchedLockers(rc.RLocker())()
	return rc.LocalMember(ctx, in, opts...)
}

func (rc *rpcClient) AddMember(ctx context.Context, in *rpcpb.AddMemberRequest, opts ...grpc.CallOption) (*rpcpb.AddMemberResponse, error) {
	defer syncutil.SchedLockers(rc.RLocker())()
	defer func() {
		if rc.agt != nil {
			_ = rc.agt.Resolve()
		}
	}()
	return rc.clusterCli.AddMember(ctx, in, opts...)
}

func (rc *rpcClient) PromoteMember(ctx context.Context, in *rpcpb.PromoteMemberRequest, opts ...grpc.CallOption) (*rpcpb.PromoteMemberResponse, error) {
	defer syncutil.SchedLockers(rc.RLocker())()
	defer func() {
		if rc.agt != nil {
			_ = rc.agt.Resolve()
		}
	}()
	return rc.clusterCli.PromoteMember(ctx, in, opts...)
}

func (rc *rpcClient) RemoveMember(ctx context.Context, in *rpcpb.RemoveMemberRequest, opts ...grpc.CallOption) (*rpcpb.RemoveMemberResponse, error) {
	defer syncutil.SchedLockers(rc.RLocker())()
	defer func() {
		if rc.agt != nil {
			_ = rc.agt.Resolve()
		}
	}()
	return rc.clusterCli.RemoveMember(ctx, in, opts...)
}

func (rc *rpcClient) ClusterStatus(ctx context.Context, in *rpcpb.ClusterStatusRequest, opts ...grpc.CallOption) (*rpcpb.ClusterStatusResponse, error) {
	defer syncutil.SchedLockers(rc.RLocker())()
	return rc.clusterCli.ClusterStatus(ctx, in, opts...)
}

func (rc *rpcClient) Status(ctx context.Context, in *rpcpb.StatusRequest, opts ...grpc.CallOption) (*rpcpb.StatusResponse, error) {
	defer syncutil.SchedLockers(rc.RLocker())()
	return rc.raftCli.Status(ctx, in, opts...)
}

func (rc *rpcClient) TransferLeader(ctx context.Context, in *rpcpb.TransferLeaderRequest, opts ...grpc.CallOption) (*rpcpb.TransferLeaderResponse, error) {
	defer syncutil.SchedLockers(rc.RLocker())()
	return rc.raftCli.TransferLeader(ctx, in, opts...)
}

func newClient(ctx context.Context, agt *agent, host string) (client *rpcClient, err error) {
	ret := &rpcClient{ctx: ctx, agt: agt, host: host, closed: true}
	err = ret.doConnect()
	return ret, err
}

func (rc *rpcClient) ID() uint64 {
	return rc.id
}

func (rc *rpcClient) Host() string {
	return rc.host
}

func (rc *rpcClient) Lock() {
	rc.RWMutex.RLock()
}
func (rc *rpcClient) Unlock() {
	rc.RWMutex.RUnlock()
}

func (rc *rpcClient) doConnect() error {
	defer syncutil.SchedLockers(&rc.RWMutex)()
	if !rc.closed {
		return nil
	}
	var err error
	rc.conn, err = grpc.DialContext(rc.ctx, rc.host, grpc.WithBlock(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	rc.kvCli = rpcpb.NewKVServiceClient(rc.conn)
	rc.clusterCli = rpcpb.NewClusterServiceClient(rc.conn)
	rc.raftCli = rpcpb.NewRaftServiceClient(rc.conn)

	if resp, err := rc.clusterCli.LocalMember(rc.ctx, new(rpcpb.LocalMemberRequest)); err != nil {
		_ = rc.conn.Close()
		rc.conn = nil
		return err
	} else {
		rc.id = resp.Member.ID
		rc.closed = false
	}
	return err
}

func (rc *rpcClient) doClose() error {
	defer syncutil.SchedLockers(&rc.RWMutex)()
	rc.closed = true
	if rc.conn == nil {
		return nil
	}
	defer func() { rc.conn = nil }()
	return rc.conn.Close()
}
