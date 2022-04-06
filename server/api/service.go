package api

import (
	"context"
	"github.com/vigilglc/raft-lsm/server"
	api "github.com/vigilglc/raft-lsm/server/api/rpcpb"
	"github.com/vigilglc/raft-lsm/server/config"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"net"
)

type serviceServer struct {
	lg *zap.Logger
	sv *server.Server
}

func StartService(cfg *config.ServerConfig) error {
	lg := cfg.GetLogger()
	sv := server.NewServer(cfg)
	sv.Start()
	defer sv.Stop()
	ss := &serviceServer{lg: lg, sv: sv}
	addr := cfg.LocalAddrInfo.ServiceAddress()
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		lg.Error("failed to listen network",
			zap.String("addr", addr), zap.Error(err),
		)
		return err
	}
	gSrv := grpc.NewServer()
	api.RegisterKVServiceServer(gSrv, ss)
	api.RegisterClusterServiceServer(gSrv, ss)
	api.RegisterRaftServiceServer(gSrv, ss)
	return gSrv.Serve(lis)
}

func (ss *serviceServer) Get(ctx context.Context, req *api.GetRequest) (resp *api.GetResponse, err error) {
	resp, err = ss.sv.Get(ctx, req)
	if err != nil {
		ss.lg.Error("failed to process request", zap.Any("request", req), zap.Error(err))
	}
	return
}

func (ss *serviceServer) Put(ctx context.Context, req *api.PutRequest) (resp *api.PutResponse, err error) {
	resp, err = ss.sv.Put(ctx, req)
	if err != nil {
		ss.lg.Error("failed to process request", zap.Any("request", req), zap.Error(err))
	}
	return
}

func (ss *serviceServer) Del(ctx context.Context, req *api.DelRequest) (resp *api.DelResponse, err error) {
	resp, err = ss.sv.Del(ctx, req)
	if err != nil {
		ss.lg.Error("failed to process request", zap.Any("request", req), zap.Error(err))
	}
	return
}

func (ss *serviceServer) Write(ctx context.Context, req *api.WriteRequest) (resp *api.WriteResponse, err error) {
	resp, err = ss.sv.Write(ctx, req)
	if err != nil {
		ss.lg.Error("failed to process request", zap.Any("request", req), zap.Error(err))
	}
	return
}

func (ss *serviceServer) Range(rangeServer api.KVService_RangeServer) (err error) {
	err = ss.sv.Range(rangeServer)
	if err != nil {
		ss.lg.Error("failed to process request", zap.Error(err))
	}
	return
}

func (ss *serviceServer) AddMember(ctx context.Context, req *api.AddMemberRequest) (resp *api.AddMemberResponse, err error) {
	resp, err = ss.sv.AddMember(ctx, req)
	if err != nil {
		ss.lg.Error("failed to process request", zap.Any("request", req), zap.Error(err))
	}
	return
}

func (ss *serviceServer) PromoteMember(ctx context.Context, req *api.PromoteMemberRequest) (resp *api.PromoteMemberResponse, err error) {
	resp, err = ss.sv.PromoteMember(ctx, req)
	if err != nil {
		ss.lg.Error("failed to process request", zap.Any("request", req), zap.Error(err))
	}
	return
}

func (ss *serviceServer) RemoveMember(ctx context.Context, req *api.RemoveMemberRequest) (resp *api.RemoveMemberResponse, err error) {
	resp, err = ss.sv.RemoveMember(ctx, req)
	if err != nil {
		ss.lg.Error("failed to process request", zap.Any("request", req), zap.Error(err))
	}
	return
}

func (ss *serviceServer) ClusterStatus(ctx context.Context, req *api.ClusterStatusRequest) (resp *api.ClusterStatusResponse, err error) {
	resp, err = ss.sv.ClusterStatus(ctx, req)
	if err != nil {
		ss.lg.Error("failed to process request", zap.Any("request", req), zap.Error(err))
	}
	return
}

func (ss *serviceServer) Status(ctx context.Context, req *api.StatusRequest) (resp *api.StatusResponse, err error) {
	resp, err = ss.sv.Status(ctx, req)
	if err != nil {
		ss.lg.Error("failed to process request", zap.Any("request", req), zap.Error(err))
	}
	return
}

func (ss *serviceServer) TransferLeader(ctx context.Context, req *api.TransferLeaderRequest) (resp *api.TransferLeaderResponse, err error) {
	resp, err = ss.sv.TransferLeader(ctx, req)
	if err != nil {
		ss.lg.Error("failed to process request", zap.Any("request", req), zap.Error(err))
	}
	return
}
