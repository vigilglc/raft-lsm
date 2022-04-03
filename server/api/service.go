package api

import (
	"context"
	"github.com/vigilglc/raft-lsm/server"
	api "github.com/vigilglc/raft-lsm/server/api/rpcpb"
	"go.uber.org/zap"
)

type ServiceServer struct {
	lg *zap.Logger
	sv *server.Server
}

func (ss *ServiceServer) Get(ctx context.Context, req *api.GetRequest) (resp *api.GetResponse, err error) {
	resp, err = ss.sv.Get(ctx, req)
	if err != nil {
		ss.lg.Error("failed to process request", zap.Any("request", req), zap.Error(err))
	}
	return
}

func (ss *ServiceServer) Put(ctx context.Context, req *api.PutRequest) (resp *api.PutResponse, err error) {
	resp, err = ss.sv.Put(ctx, req)
	if err != nil {
		ss.lg.Error("failed to process request", zap.Any("request", req), zap.Error(err))
	}
	return
}

func (ss *ServiceServer) Del(ctx context.Context, req *api.DelRequest) (resp *api.DelResponse, err error) {
	resp, err = ss.sv.Del(ctx, req)
	if err != nil {
		ss.lg.Error("failed to process request", zap.Any("request", req), zap.Error(err))
	}
	return
}

func (ss *ServiceServer) Write(ctx context.Context, req *api.WriteRequest) (resp *api.WriteResponse, err error) {
	resp, err = ss.sv.Write(ctx, req)
	if err != nil {
		ss.lg.Error("failed to process request", zap.Any("request", req), zap.Error(err))
	}
	return
}

func (ss *ServiceServer) Range(rangeServer api.KVService_RangeServer) (err error) {
	err = ss.sv.Range(rangeServer)
	if err != nil {
		ss.lg.Error("failed to process request", zap.Error(err))
	}
	return
}

func (ss *ServiceServer) AddMember(ctx context.Context, req *api.AddMemberRequest) (resp *api.AddMemberResponse, err error) {
	resp, err = ss.sv.AddMember(ctx, req)
	if err != nil {
		ss.lg.Error("failed to process request", zap.Any("request", req), zap.Error(err))
	}
	return
}

func (ss *ServiceServer) PromoteMember(ctx context.Context, req *api.PromoteMemberRequest) (resp *api.PromoteMemberResponse, err error) {
	resp, err = ss.sv.PromoteMember(ctx, req)
	if err != nil {
		ss.lg.Error("failed to process request", zap.Any("request", req), zap.Error(err))
	}
	return
}

func (ss *ServiceServer) RemoveMember(ctx context.Context, req *api.RemoveMemberRequest) (resp *api.RemoveMemberResponse, err error) {
	resp, err = ss.sv.RemoveMember(ctx, req)
	if err != nil {
		ss.lg.Error("failed to process request", zap.Any("request", req), zap.Error(err))
	}
	return
}

func (ss *ServiceServer) ClusterStatus(ctx context.Context, req *api.ClusterStatusRequest) (resp *api.ClusterStatusResponse, err error) {
	resp, err = ss.sv.ClusterStatus(ctx, req)
	if err != nil {
		ss.lg.Error("failed to process request", zap.Any("request", req), zap.Error(err))
	}
	return
}

func (ss *ServiceServer) Status(ctx context.Context, req *api.StatusRequest) (resp *api.StatusResponse, err error) {
	resp, err = ss.sv.Status(ctx, req)
	if err != nil {
		ss.lg.Error("failed to process request", zap.Any("request", req), zap.Error(err))
	}
	return
}

func (ss *ServiceServer) TransferLeader(ctx context.Context, req *api.TransferLeaderRequest) (resp *api.TransferLeaderResponse, err error) {
	resp, err = ss.sv.TransferLeader(ctx, req)
	if err != nil {
		ss.lg.Error("failed to process request", zap.Any("request", req), zap.Error(err))
	}
	return
}
