package bootstrap

import (
	"errors"
	"github.com/vigilglc/raft-lsm/server/backend"
	"github.com/vigilglc/raft-lsm/server/cluster"
	"github.com/vigilglc/raft-lsm/server/config"
	"go.uber.org/zap"
)

type BootstrappedCluster struct {
	Cluster *cluster.Cluster
	Remotes []*cluster.Member
}

func bootstrapCluster(cfg *config.ServerConfig, haveWAL bool, be backend.Backend) (btCl *BootstrappedCluster, err error) {
	switch {
	case !haveWAL: // newly or existing launched cluster, new node to join
		return bootstrapClusterWithoutWAL(cfg, be) // TODO: fetch cluster ID from online Nodes...
	case haveWAL && !cfg.NewCluster: // existing cluster, e.g. old node to restart
		return bootstrapExistingClusterWithWAL(cfg, be)
	default:
		return nil, errors.New("unsupported bootstrap config")
	}
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
	if err := cl.PushMembers2Backend(); err != nil {
		lg.Error("failed to push members to backend", zap.Error(err))
		return nil, err
	}
	return &BootstrappedCluster{
		Cluster: cl,
		Remotes: nil, // TODO: fetch remotes from other online nodes...
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
