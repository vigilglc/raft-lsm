package bootstrap

import (
	"github.com/vigilglc/raft-lsm/server/config"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.etcd.io/etcd/server/v3/wal"
	"go.uber.org/zap"
)

func bootstrapSnapshot(cfg *config.ServerConfig, haveWAL bool) (snapshotter *snap.Snapshotter, snapshot *raftpb.Snapshot) {
	lg := cfg.GetLogger()
	cfg.MakeSnapshotterDir()
	snapshotter = snap.New(lg, cfg.GetSnapshotterDir())
	if haveWAL {
		walSnaps, err := wal.ValidSnapshotEntries(lg, cfg.GetWALDir())
		if err != nil {
			lg.Fatal("failed to read valid snapshot entries from WAL",
				zap.String("wal-dir", cfg.GetWALDir()), zap.Error(err),
			)
		}
		snapshot, err = snapshotter.LoadNewestAvailable(walSnaps)
		if err != nil && err != snap.ErrNoSnapshot {
			lg.Fatal("failed to load newest available snapshot", zap.Error(err))
		}
	}
	return
}
