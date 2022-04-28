package bootstrap

import (
	"github.com/vigilglc/raft-lsm/server/backend"
	"github.com/vigilglc/raft-lsm/server/config"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.etcd.io/etcd/server/v3/wal"
	"go.uber.org/zap"
)

type BootstrappedServer struct {
	Snapshotter         *snap.Snapshotter
	Backend             backend.Backend
	WAL                 *wal.WAL
	MemStorage          *raft.MemoryStorage
	BootstrappedCluster *BootstrappedCluster
	BootstrappedRaft    *BootstrappedRaft
}

func (srv *BootstrappedServer) Close() error {
	var err error
	if srv.Backend != nil {
		err = srv.Backend.Close()
	}
	if srv.WAL != nil {
		if er := srv.WAL.Close(); err == nil && er != nil {
			err = er
		}
	}
	return err
}

func Bootstrap(cfg *config.ServerConfig) (srv *BootstrappedServer, berr error) {
	var setBerr = func(err error) {
		if berr == nil {
			berr = err
		}
	}
	lg := cfg.GetLogger()
	cfg.MakeDataDir()

	haveWAL := wal.Exist(cfg.GetWALDir())
	snapshotter, snapshot := bootstrapSnapshot(cfg, haveWAL)
	be := bootstrapBackend(cfg, snapshotter, snapshot)

	btWAL, err := bootstrapWAL(cfg, haveWAL, snapshot)
	if err != nil {
		setBerr(err)
		lg.Error("failed to bootstrap WAL", zap.Error(err))
		return
	}

	memStorage, err := btWAL.bootstrapMemoryStorage()
	if err != nil {
		setBerr(err)
		lg.Error("failed to bootstrap memory Storage", zap.Error(err))
		return
	}
	btCl, err := bootstrapCluster(cfg, btWAL, be)
	if err != nil {
		setBerr(err)
		lg.Error("failed to bootstrap cluster", zap.Error(err))
		return
	}
	btRaft, err := bootstrapRaft(cfg, haveWAL, btCl.Cluster, memStorage)
	if err != nil {
		setBerr(err)
		lg.Error("failed to bootstrap raft", zap.Error(err))
		return
	}
	if !haveWAL {
		if err := btWAL.createWAL(cfg, btCl.Cluster.GetClusterName(), btCl.Cluster.GetClusterID(),
			btCl.Cluster.GetLocalMember().ID); err != nil {
			setBerr(err)
			lg.Error("failed to create WAL", zap.Error(err))
			return
		}
	}
	srv = &BootstrappedServer{
		Snapshotter:         snapshotter,
		Backend:             be,
		WAL:                 btWAL.wal,
		MemStorage:          memStorage,
		BootstrappedCluster: btCl,
		BootstrappedRaft:    btRaft,
	}
	return
}
