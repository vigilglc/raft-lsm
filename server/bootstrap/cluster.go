package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"github.com/vigilglc/raft-lsm/server/api/rpcpb"
	"github.com/vigilglc/raft-lsm/server/backend"
	"github.com/vigilglc/raft-lsm/server/cluster"
	"github.com/vigilglc/raft-lsm/server/config"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type BootstrappedCluster struct {
	Cluster *cluster.Cluster
	Remotes []*cluster.Member
}

func bootstrapCluster(cfg *config.ServerConfig, haveWAL bool, be backend.Backend) (btCl *BootstrappedCluster, err error) {
	switch {
	case !haveWAL: // newly or existing launched cluster, new node to join
		return bootstrapClusterWithoutWAL(cfg, be)
	case haveWAL && !cfg.NewCluster: // existing cluster, e.g. old node to restart
		return bootstrapExistingClusterWithWAL(cfg, be)
	default:
		return nil, errors.New("unsupported bootstrap config")
	}
}

func fetchRemoteClusterStatus(cfg *config.ServerConfig, members []*cluster.Member) (status *rpcpb.ClusterStatusResponse, err error) {
	var failed = true
	for _, mem := range members {
		cCtx, cCancel := context.WithTimeout(context.Background(), cfg.GetPeerDialTimeout())
		conn, err := grpc.DialContext(cCtx, mem.ServiceAddress(), grpc.WithBlock())
		if err != nil {
			cCancel()
			continue
		}
		client := rpcpb.NewClusterServiceClient(conn)
		rCtx, rCancel := context.WithTimeout(context.Background(), cfg.GetRequestTimeout())
		resp, err := client.ClusterStatus(rCtx, &rpcpb.ClusterStatusRequest{Linearizable: true})
		if err == nil && len(resp.Members) > 0 {
			failed = false
			status = resp
		}
		rCancel()
		cCancel()
		_ = conn.Close()
		if !failed {
			return status, nil
		}
	}
	return nil, fmt.Errorf("could not fetch cluster status from remote members")
}

func bootstrapClusterWithoutWAL(cfg *config.ServerConfig, be backend.Backend) (btCl *BootstrappedCluster, err error) {
	lg := cfg.GetLogger()
	localMem := cluster.NewMember(cfg.ClusterName, cfg.LocalAddrInfo, false)
	cl, err := cluster.NewClusterBuilder(lg, cfg.ClusterName, be).
		AddMember(cfg.LocalAddrInfo).SetLocalMember(localMem.ID).
		AddMembers(cfg.PeerAddrInfos...).Finish()
	if err != nil {
		return nil, err
	}
	var remotes []*cluster.Member
	if cfg.NewCluster {
		members := cl.GetMembers()
		status, err := fetchRemoteClusterStatus(cfg, members)
		if err != nil {
			return nil, err
		}
		cl.SetClusterName(status.Name)
		cl.SetClusterID(status.ID)
		var memMap = map[uint64]*cluster.Member{}
		for _, mem := range members {
			memMap[mem.ID] = mem
		}
		for _, pbMem := range status.Members {
			if _, ok := memMap[pbMem.ID]; !ok {
				remotes = append(remotes, cluster.NewMember(status.Name, cluster.AddrInfo{
					Name:        pbMem.Name,
					Host:        pbMem.Host,
					ServicePort: uint16(pbMem.ServicePort),
					RaftPort:    uint16(pbMem.RaftPort),
				}, false))
			}
		}
	}
	if err := cl.PushMembers2Backend(); err != nil {
		lg.Error("failed to push members to backend", zap.Error(err))
		return nil, err
	}
	return &BootstrappedCluster{
		Cluster: cl,
		Remotes: remotes,
	}, err
}

func bootstrapExistingClusterWithWAL(cfg *config.ServerConfig, be backend.Backend) (btCl *BootstrappedCluster, err error) {
	lg := cfg.GetLogger()
	localMem := cluster.NewMember(cfg.ClusterName, cfg.LocalAddrInfo, false)
	cl, err := cluster.NewClusterBuilder(lg, cfg.ClusterName, be).SetLocalMember(localMem.ID).Finish()
	if err != nil {
		return nil, err
	}
	if err := cl.RecoverMembers(); err != nil {
		return nil, err
	}
	return &BootstrappedCluster{
		Cluster: cl,
		Remotes: nil,
	}, err
}
