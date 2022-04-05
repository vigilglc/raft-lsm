package client

import (
	"context"
	"errors"
	"github.com/vigilglc/raft-lsm/server/api/rpcpb"
	"github.com/vigilglc/raft-lsm/server/utils/syncutil"
	"google.golang.org/grpc"
	"sync"
)

var (
	ErrClientClosed = errors.New("client: closed")
)

type Client interface {
	rpcpb.KVServiceClient
	rpcpb.ClusterServiceClient
	rpcpb.RaftServiceClient
	Host() string
	Closed() bool
	Reset() error
	Close() error
}

type rpcClient struct {
	ctx  context.Context
	host string

	rwmu       sync.RWMutex
	conn       *grpc.ClientConn
	closed     bool
	kvCli      rpcpb.KVServiceClient
	clusterCli rpcpb.ClusterServiceClient
	raftCli    rpcpb.RaftServiceClient
}

func (rc *rpcClient) Get(ctx context.Context, in *rpcpb.GetRequest, opts ...grpc.CallOption) (*rpcpb.GetResponse, error) {
	defer syncutil.SchedLockers(rc.rwmu.RLocker())()
	if rc.closed {
		return nil, ErrClientClosed
	}
	return rc.kvCli.Get(ctx, in, opts...)
}

func (rc *rpcClient) Put(ctx context.Context, in *rpcpb.PutRequest, opts ...grpc.CallOption) (*rpcpb.PutResponse, error) {
	defer syncutil.SchedLockers(rc.rwmu.RLocker())()
	if rc.closed {
		return nil, ErrClientClosed
	}
	return rc.kvCli.Put(ctx, in, opts...)
}

func (rc *rpcClient) Del(ctx context.Context, in *rpcpb.DelRequest, opts ...grpc.CallOption) (*rpcpb.DelResponse, error) {
	defer syncutil.SchedLockers(rc.rwmu.RLocker())()
	if rc.closed {
		return nil, ErrClientClosed
	}
	return rc.kvCli.Del(ctx, in, opts...)
}

func (rc *rpcClient) Write(ctx context.Context, in *rpcpb.WriteRequest, opts ...grpc.CallOption) (*rpcpb.WriteResponse, error) {
	defer syncutil.SchedLockers(rc.rwmu.RLocker())()
	if rc.closed {
		return nil, ErrClientClosed
	}
	return rc.kvCli.Write(ctx, in, opts...)
}

func (rc *rpcClient) Range(ctx context.Context, opts ...grpc.CallOption) (rpcpb.KVService_RangeClient, error) {
	defer syncutil.SchedLockers(rc.rwmu.RLocker())()
	if rc.closed {
		return nil, ErrClientClosed
	}
	return rc.kvCli.Range(ctx, opts...)
}

func (rc *rpcClient) AddMember(ctx context.Context, in *rpcpb.AddMemberRequest, opts ...grpc.CallOption) (*rpcpb.AddMemberResponse, error) {
	defer syncutil.SchedLockers(rc.rwmu.RLocker())()
	if rc.closed {
		return nil, ErrClientClosed
	}
	return rc.clusterCli.AddMember(ctx, in, opts...)
}

func (rc *rpcClient) PromoteMember(ctx context.Context, in *rpcpb.PromoteMemberRequest, opts ...grpc.CallOption) (*rpcpb.PromoteMemberResponse, error) {
	defer syncutil.SchedLockers(rc.rwmu.RLocker())()
	if rc.closed {
		return nil, ErrClientClosed
	}
	return rc.clusterCli.PromoteMember(ctx, in, opts...)
}

func (rc *rpcClient) RemoveMember(ctx context.Context, in *rpcpb.RemoveMemberRequest, opts ...grpc.CallOption) (*rpcpb.RemoveMemberResponse, error) {
	defer syncutil.SchedLockers(rc.rwmu.RLocker())()
	if rc.closed {
		return nil, ErrClientClosed
	}
	return rc.clusterCli.RemoveMember(ctx, in, opts...)
}

func (rc *rpcClient) ClusterStatus(ctx context.Context, in *rpcpb.ClusterStatusRequest, opts ...grpc.CallOption) (*rpcpb.ClusterStatusResponse, error) {
	defer syncutil.SchedLockers(rc.rwmu.RLocker())()
	if rc.closed {
		return nil, ErrClientClosed
	}
	return rc.clusterCli.ClusterStatus(ctx, in, opts...)
}

func (rc *rpcClient) Status(ctx context.Context, in *rpcpb.StatusRequest, opts ...grpc.CallOption) (*rpcpb.StatusResponse, error) {
	defer syncutil.SchedLockers(rc.rwmu.RLocker())()
	if rc.closed {
		return nil, ErrClientClosed
	}
	return rc.raftCli.Status(ctx, in, opts...)
}

func (rc *rpcClient) TransferLeader(ctx context.Context, in *rpcpb.TransferLeaderRequest, opts ...grpc.CallOption) (*rpcpb.TransferLeaderResponse, error) {
	defer syncutil.SchedLockers(rc.rwmu.RLocker())()
	if rc.closed {
		return nil, ErrClientClosed
	}
	return rc.raftCli.TransferLeader(ctx, in, opts...)
}

func NewClient(ctx context.Context, host string) (client Client) {
	return &rpcClient{ctx: ctx, host: host, closed: true}
}
func (rc *rpcClient) Host() string {
	defer syncutil.SchedLockers(rc.rwmu.RLocker())()
	return rc.host
}

func (rc *rpcClient) Reset() error {
	defer syncutil.SchedLockers(&rc.rwmu)()
	var err error
	if rc.conn != nil && !rc.closed {
		if err = rc.conn.Close(); err != nil {
			return err
		}
	}
	rc.conn, err = grpc.DialContext(rc.ctx, rc.host)
	if err != nil {
		return err
	}
	rc.closed = false
	rc.kvCli = rpcpb.NewKVServiceClient(rc.conn)
	rc.clusterCli = rpcpb.NewClusterServiceClient(rc.conn)
	rc.raftCli = rpcpb.NewRaftServiceClient(rc.conn)
	return err
}

func (rc *rpcClient) Closed() bool {
	defer syncutil.SchedLockers(rc.rwmu.RLocker())()
	return rc.closed
}

func (rc *rpcClient) Close() error {
	defer syncutil.SchedLockers(&rc.rwmu)()
	if rc.closed {
		return nil
	}
	rc.closed = true
	return rc.conn.Close()
}
