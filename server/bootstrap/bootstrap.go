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

func Bootstrap(cfg *config.ServerConfig) (srv *BootstrappedServer) {
	var failed = true
	lg := cfg.GetLogger()
	cfg.MakeDataDir()
	defer func() {
		if failed {
			lg.Fatal("failed to bootstrap server")
		}
	}()
	haveWAL := wal.Exist(cfg.GetWALDir())
	snapshotter, snapshot := bootstrapSnapshot(cfg, haveWAL)
	be := bootstrapBackend(cfg, snapshotter, snapshot)
	defer func() {
		if failed {
			_ = be.Close()
		}
	}()

	btWAL, err := bootstrapWAL(cfg, haveWAL, snapshot)
	if err != nil {
		lg.Error("failed to bootstrap WAL", zap.Error(err))
		return
	}
	memStorage, err := btWAL.bootstrapMemoryStorage()
	if err != nil {
		lg.Error("failed to bootstrap memory Storage", zap.Error(err))
		return nil
	}
	btCl, err := bootstrapCluster(cfg, haveWAL, be)
	if err != nil {
		lg.Error("failed to bootstrap cluster", zap.Error(err))
		return
	}
	btRaft, err := bootstrapRaft(cfg, haveWAL, btCl.Cluster, memStorage)
	if err != nil {
		lg.Error("failed to bootstrap raft", zap.Error(err))
		return nil
	}
	srv = &BootstrappedServer{
		Snapshotter:         snapshotter,
		Backend:             be,
		WAL:                 btWAL.wal,
		MemStorage:          memStorage,
		BootstrappedCluster: btCl,
		BootstrappedRaft:    btRaft,
	}
	failed = false
	return srv
}
