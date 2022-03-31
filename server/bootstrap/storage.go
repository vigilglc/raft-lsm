package bootstrap

import (
	json "github.com/json-iterator/go"
	"github.com/vigilglc/raft-lsm/server/cluster"
	"github.com/vigilglc/raft-lsm/server/config"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/wal"
	"go.etcd.io/etcd/server/v3/wal/walpb"
	"go.uber.org/zap"
	"io"
)

type bootstrappedWAL struct {
	haveWAL   bool
	wal       *wal.WAL
	snap      *raftpb.Snapshot
	walMeta   *walMeta
	hardState raftpb.HardState
	entries   []raftpb.Entry
}

type walMeta struct {
	clusterName string
	localNodeID uint64
}

func encodeWalMeta(wm walMeta) ([]byte, error) {
	return json.Marshal(&wm)
}

func decodeWalMeta(data []byte) (wm *walMeta, err error) {
	wm = new(walMeta)
	err = json.Unmarshal(data, wm)
	return
}

func bootstrapWAL(cfg *config.ServerConfig, haveWAL bool, snap *raftpb.Snapshot) (*bootstrappedWAL, error) {
	lg := cfg.GetLogger()
	cfg.MakeWALDir()
	if !haveWAL {
		nodeID := cluster.ComputeMemberID(cfg.ClusterName, cfg.LocalAddrInfo)
		walMeta := walMeta{clusterName: cfg.ClusterName, localNodeID: nodeID}
		wmDat, err := encodeWalMeta(walMeta)
		if err != nil {
			lg.Error("failed to encode walMeta", zap.Error(err))
			return nil, err
		}
		w, err := wal.Create(lg, cfg.GetWALDir(), wmDat)
		if err != nil {
			lg.Error("failed to create WAL", zap.Error(err))
			return nil, err
		}

		return &bootstrappedWAL{
			haveWAL: haveWAL,
			wal:     w,
			walMeta: &walMeta,
		}, nil
	}
	var walSnap walpb.Snapshot
	if snap != nil {
		walSnap.Index, walSnap.Term = snap.Metadata.Index, snap.Metadata.Term
	}
	repaired := false
	for {
		w, err := wal.Open(lg, cfg.GetWALDir(), walSnap)
		if err != nil {
			lg.Error("failed to open WAL", zap.Error(err))
			return nil, err
		}
		wmDat, hardState, ents, err := w.ReadAll()
		if err != nil {
			_ = w.Close()
			// we can only repair ErrUnexpectedEOF, and we never repair twice.
			if repaired || err != io.ErrUnexpectedEOF {
				lg.Error("failed to read WAL, cannot be repaired", zap.Error(err))
				return nil, err
			}
			if !wal.Repair(lg, cfg.GetWALDir()) {
				lg.Error("failed to repair WAL", zap.Error(err))
				return nil, err
			} else {
				lg.Info("repaired WAL", zap.Error(err))
				repaired = true
			}
			continue
		}
		walMeta, err := decodeWalMeta(wmDat)
		if err != nil {
			lg.Error("failed to decode walMeta", zap.Error(err))
			return nil, err
		}
		return &bootstrappedWAL{
			haveWAL:   haveWAL,
			wal:       w,
			snap:      snap,
			walMeta:   walMeta,
			hardState: hardState,
			entries:   ents,
		}, nil
	}
}

func (btWAL *bootstrappedWAL) bootstrapMemoryStorage() (memStorage *raft.MemoryStorage, err error) {
	memStorage = raft.NewMemoryStorage()
	if btWAL.snap != nil {
		if err = memStorage.ApplySnapshot(*btWAL.snap); err != nil {
			return nil, err
		}
	}
	if err = memStorage.SetHardState(btWAL.hardState); err != nil {
		return nil, err
	}

	if err = memStorage.Append(btWAL.entries); err != nil {
		return nil, err
	}
	return memStorage, nil
}
