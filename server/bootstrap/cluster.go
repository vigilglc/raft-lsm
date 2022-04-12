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

func bootstrapCluster(cfg *config.ServerConfig, btWAL *bootstrappedWAL, be backend.Backend) (btCl *BootstrappedCluster, err error) {
	switch {
	case !btWAL.haveWAL: // newly or existing launched cluster, new node to join
		return bootstrapClusterWithoutWAL(cfg, btWAL, be)
	case btWAL.haveWAL && !cfg.NewCluster: // existing cluster, e.g. old node to restart
		return bootstrapExistingClusterWithWAL(cfg, btWAL.walMeta, be)
	default:
		return nil, errors.New("unsupported bootstrap config")
	}
}

type clusterStatusFetcher func(cfg *config.ServerConfig, mem *cluster.Member) (status *rpcpb.ClusterStatusResponse, err error)

var defaultClusterStatusFetcher clusterStatusFetcher = func(cfg *config.ServerConfig, mem *cluster.Member) (
	status *rpcpb.ClusterStatusResponse, err error) {
	lg := cfg.GetLogger()
	cCtx, cCancel := context.WithTimeout(context.Background(), cfg.GetPeerDialTimeout())
	defer cCancel()
	conn, err := grpc.DialContext(cCtx, mem.ServiceAddress())
	if err != nil {
		lg.Info("failed to connect to raft service", zap.String("addr", mem.ServiceAddress()), zap.Error(err))
		return nil, err
	}
	client := rpcpb.NewClusterServiceClient(conn)
	rCtx, rCancel := context.WithTimeout(context.Background(), cfg.GetRequestTimeout())
	defer rCancel()
	resp, err := client.ClusterStatus(rCtx, &rpcpb.ClusterStatusRequest{Linearizable: true})
	if err != nil {
		lg.Info("failed to receive response", zap.String("addr", mem.ServiceAddress()), zap.Error(err))
		return nil, err
	}
	if len(resp.Members) == 0 {
		return nil, fmt.Errorf("got no members from %s", mem.ServiceAddress())
	}
	return resp, nil
}

func fetchRemoteClusterStatus(cfg *config.ServerConfig, members []*cluster.Member) (status *rpcpb.ClusterStatusResponse, err error) {
	for _, mem := range members {
		if resp, err := defaultClusterStatusFetcher(cfg, mem); err == nil {
			return resp, err
		}
	}
	return nil, fmt.Errorf("could not fetch cluster status from remote members")
}

func differentiateRemotes(localMems []*cluster.Member, status *rpcpb.ClusterStatusResponse) (remotes []*cluster.Member) {
	var memMap = map[uint64]*cluster.Member{}
	for _, mem := range localMems {
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
	return remotes
}

func bootstrapClusterWithoutWAL(cfg *config.ServerConfig, btWAL *bootstrappedWAL, be backend.Backend) (btCl *BootstrappedCluster, err error) {
	lg := cfg.GetLogger()
	localMem := cluster.NewMember(cfg.ClusterName, cfg.LocalAddrInfo, false)
	cl, err := cluster.NewClusterBuilder(lg, cfg.ClusterName, be).
		AddMember(cfg.LocalAddrInfo).SetLocalMember(localMem.ID).
		AddMembers(cfg.PeerAddrInfos...).Finish()
	if err != nil {
		return nil, err
	}
	var remotes []*cluster.Member
	if !cfg.NewCluster {
		members := cl.GetMembers()
		status, err := fetchRemoteClusterStatus(cfg, members)
		if err != nil {
			return nil, err
		}
		cl.SetClusterName(status.Name)
		cl.SetClusterID(status.ID)
		remotes = differentiateRemotes(members, status)
	}
	if err := cl.PushMembers2Backend(); err != nil {
		lg.Error("failed to push members to backend", zap.Error(err))
		return nil, err
	}
	if err := btWAL.createWAL(cfg, cl.GetClusterName(), cl.GetClusterID(), localMem.ID); err != nil {
		lg.Error("failed to create WAL", zap.Error(err))
		return nil, err
	}
	return &BootstrappedCluster{
		Cluster: cl,
		Remotes: remotes,
	}, err
}

func bootstrapExistingClusterWithWAL(cfg *config.ServerConfig, meta *walMeta, be backend.Backend) (btCl *BootstrappedCluster, err error) {
	lg := cfg.GetLogger()
	localMem := cluster.NewMember(cfg.ClusterName, cfg.LocalAddrInfo, false)
	cl, err := cluster.NewClusterBuilder(lg, cfg.ClusterName, be).SetLocalMember(localMem.ID).Finish()
	if err != nil {
		return nil, err
	}
	cl.SetClusterName(meta.clusterName)
	cl.SetClusterID(meta.clusterID)
	if err := cl.RecoverMembers(); err != nil {
		return nil, err
	}
	members := cl.GetMembers()
	status, err := fetchRemoteClusterStatus(cfg, members)
	if err != nil {
		return nil, err
	}
	remotes := differentiateRemotes(members, status)
	return &BootstrappedCluster{
		Cluster: cl,
		Remotes: remotes,
	}, err
}
