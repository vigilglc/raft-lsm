package main

import (
	"context"
	"fmt"
	"github.com/vigilglc/raft-lsm/client"
	"github.com/vigilglc/raft-lsm/server/api/rpcpb"
	"github.com/vigilglc/raft-lsm/server/backend/kvpb"
	"github.com/vigilglc/raft-lsm/server/utils/mathutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"log"
)

type fakeClient struct {
	host   string
	closed bool
}

func newFakeClient(host string) client.Client {
	return &fakeClient{host: host}
}

var fakeClientError = fmt.Errorf("fake client: test error")

// region fake client impls...

func (f *fakeClient) Get(ctx context.Context, in *rpcpb.GetRequest, opts ...grpc.CallOption) (*rpcpb.GetResponse, error) {
	log.Printf("req: %v\n", in)
	if in.Key == "key" {
		return &rpcpb.GetResponse{Val: "val"}, nil
	}
	return new(rpcpb.GetResponse), fakeClientError
}

func (f *fakeClient) Put(ctx context.Context, in *rpcpb.PutRequest, opts ...grpc.CallOption) (*rpcpb.PutResponse, error) {
	log.Printf("req: %v\n", in)
	if in.KeyVal.Key == "key" {
		return new(rpcpb.PutResponse), nil
	}
	return new(rpcpb.PutResponse), fakeClientError
}

func (f *fakeClient) Del(ctx context.Context, in *rpcpb.DelRequest, opts ...grpc.CallOption) (*rpcpb.DelResponse, error) {
	log.Printf("req: %v\n", in)
	if in.Key == "key" {
		return new(rpcpb.DelResponse), nil
	}
	return new(rpcpb.DelResponse), fakeClientError
}

func (f *fakeClient) Write(ctx context.Context, in *rpcpb.WriteRequest, opts ...grpc.CallOption) (*rpcpb.WriteResponse, error) {
	log.Printf("req: %v\n", in)
	return new(rpcpb.WriteResponse), fakeClientError
}

type fakeRangeClient struct {
	began  bool
	closed bool

	index int
	kvs   []*kvpb.KV

	resp *rpcpb.RangeResponse
	err  error
}

// region fake range client impls...

func (f *fakeRangeClient) Send(req *rpcpb.RangeRequest) error {
	log.Printf("req: %v\n", req)
	f.err = fakeClientError
	switch req.OP {
	case rpcpb.RangeRequest_BEGIN:
		if f.began {
			break
		}
		f.began = true
		f.closed = false
		f.index = 0
		f.resp, f.err = &rpcpb.RangeResponse{HasMore: true}, nil
		return nil
	case rpcpb.RangeRequest_NEXTN:
		if f.closed || !f.began {
			break
		}
		cnt := mathutil.MinUint64(uint64(len(f.kvs)-f.index), req.Count)
		f.index += int(cnt)
		f.resp = &rpcpb.RangeResponse{KeyVals: f.kvs[f.index-int(cnt) : f.index], HasMore: f.index < len(f.kvs)}
		f.err = nil
		return nil
	case rpcpb.RangeRequest_CLOSE:
		if !f.began || f.closed {
			break
		}
		f.began = false
		f.closed = true
		f.resp = &rpcpb.RangeResponse{HasMore: false}
		f.err = nil
	}
	return fakeClientError
}

func (f *fakeRangeClient) Recv() (*rpcpb.RangeResponse, error) {
	defer func() { f.err = fakeClientError }()
	return f.resp, f.err
}

func (f *fakeRangeClient) CloseSend() error {
	return nil
}

// region useless

func (f *fakeRangeClient) Header() (metadata.MD, error) {
	panic("implement me")
}

func (f *fakeRangeClient) Trailer() metadata.MD {
	panic("implement me")
}

func (f *fakeRangeClient) Context() context.Context {
	panic("implement me")
}

func (f *fakeRangeClient) SendMsg(m interface{}) error {
	panic("implement me")
}

func (f *fakeRangeClient) RecvMsg(m interface{}) error {
	panic("implement me")
}

// endregion

// endregion

func (f *fakeClient) Range(ctx context.Context, opts ...grpc.CallOption) (rpcpb.KVService_RangeClient, error) {
	ret := &fakeRangeClient{
		began:  false,
		closed: false,
		index:  0,
		err:    fakeClientError,
	}
	for ch := 'a'; ch <= 'z'; ch++ {
		ret.kvs = append(ret.kvs, &kvpb.KV{Key: string(ch), Val: string(ch)})
	}
	return ret, nil
}

func (f *fakeClient) AddMember(ctx context.Context, in *rpcpb.AddMemberRequest, opts ...grpc.CallOption) (*rpcpb.AddMemberResponse, error) {
	log.Printf("req: %v\n", in)
	if in.Member.Name == "member" {
		return new(rpcpb.AddMemberResponse), nil
	}
	return new(rpcpb.AddMemberResponse), fakeClientError
}

func (f *fakeClient) PromoteMember(ctx context.Context, in *rpcpb.PromoteMemberRequest, opts ...grpc.CallOption) (*rpcpb.PromoteMemberResponse, error) {
	log.Printf("req: %v\n", in)
	if in.NodeID == 1 {
		return new(rpcpb.PromoteMemberResponse), nil
	}
	return new(rpcpb.PromoteMemberResponse), fakeClientError
}

func (f *fakeClient) RemoveMember(ctx context.Context, in *rpcpb.RemoveMemberRequest, opts ...grpc.CallOption) (*rpcpb.RemoveMemberResponse, error) {
	log.Printf("req: %v\n", in)
	if in.NodeID == 1 {
		return new(rpcpb.RemoveMemberResponse), nil
	}
	return new(rpcpb.RemoveMemberResponse), fakeClientError
}

func (f *fakeClient) ClusterStatus(ctx context.Context, in *rpcpb.ClusterStatusRequest, opts ...grpc.CallOption) (*rpcpb.ClusterStatusResponse, error) {
	log.Printf("req: %v\n", in)
	return &rpcpb.ClusterStatusResponse{
		ID:   1,
		Name: "node1",
		Members: []*rpcpb.Member{
			{
				ID:          1,
				Name:        "node1",
				Host:        "localhost",
				RaftPort:    8010,
				ServicePort: 8090,
				IsLearner:   false,
			},
		},
		RemovedIDs: []uint64{7},
	}, nil
}

func (f *fakeClient) Status(ctx context.Context, in *rpcpb.StatusRequest, opts ...grpc.CallOption) (*rpcpb.StatusResponse, error) {
	log.Printf("req: %v\n", in)
	return &rpcpb.StatusResponse{
		NodeID:         1,
		RaftState:      rpcpb.RaftState_Leader,
		Term:           10,
		Lead:           1,
		AppliedIndex:   56,
		CommittedIndex: 60,
		Progresses: []*rpcpb.RaftProgress{
			{
				NodeID:       2,
				MatchIndex:   60,
				NextIndex:    60,
				State:        rpcpb.ProgressState_Replicate,
				RecentActive: true,
				IsLearner:    false,
			},
		},
	}, nil
}

func (f *fakeClient) TransferLeader(ctx context.Context, in *rpcpb.TransferLeaderRequest, opts ...grpc.CallOption) (*rpcpb.TransferLeaderResponse, error) {
	log.Printf("req: %v\n", in)
	if in.TransfereeID == 1 {
		return new(rpcpb.TransferLeaderResponse), nil
	}
	return new(rpcpb.TransferLeaderResponse), fakeClientError
}

func (f *fakeClient) Host() string {
	return f.host
}

func (f *fakeClient) Closed() bool {
	return f.closed
}

func (f *fakeClient) Reset() error {
	f.closed = false
	return nil
}

func (f *fakeClient) Close() error {
	f.closed = true
	return nil
}

// endregion
