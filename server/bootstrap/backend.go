package bootstrap

import (
	"github.com/vigilglc/raft-lsm/server/backend"
	"github.com/vigilglc/raft-lsm/server/config"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.uber.org/zap"
	"os"
)

func bootstrapBackend(cfg *config.ServerConfig, snapshotter *snap.Snapshotter, snapshot *raftpb.Snapshot) backend.Backend {
	lg := cfg.GetLogger()
	be, err := backend.OpenBackend(lg, cfg.GetBackendConfig())
	if err != nil {
		if be != nil {
			_ = be.Close()
		}
		lg.Fatal("failed to open backend", zap.Error(err))
	}

	if snapshot != nil {
		if err := tryRecoverBackend(lg, be, snapshotter, snapshot); err != nil {
			_ = be.Close()
			lg.Fatal("failed to recover backend from snapshot", zap.Error(err))
		}
	}
	return be
}

func tryRecoverBackend(lg *zap.Logger, be backend.Backend, ss *snap.Snapshotter, snap *raftpb.Snapshot) error {
	index, appliedIndex := snap.Metadata.Index, be.AppliedIndex()
	if index <= appliedIndex {
		return nil
	}
	lg.Info(
		"recovering snapshot", zap.Uint64("current-applied-index", appliedIndex),
		zap.Uint64("incoming-snapshot-index", index),
	)
	dbFn, err := ss.DBFilePath(index)
	if err != nil {
		lg.Error("failed to get DB file path", zap.Uint64("snap-index", index), zap.Error(err))
		return err
	}
	f, err := os.Open(dbFn)
	if err != nil {
		lg.Error("failed to open DB file", zap.String("db-filename", dbFn), zap.Error(err))
		return err
	}
	if err := be.ReceiveSnapshot(index, f); err != nil {
		_ = f.Close()
		lg.Error("backend failed to receive snapshot", zap.Error(err))
		return err
	}
	if err := f.Close(); err != nil {
		lg.Error("failed to close DB file", zap.String("DB-filename", dbFn), zap.Error(err))
		return err
	}
	return nil
}
