package server

import (
	"github.com/vigilglc/raft-lsm/server/api"
	"github.com/vigilglc/raft-lsm/server/raftn"
	"github.com/vigilglc/raft-lsm/server/utils/mathutil"
	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.uber.org/zap"
	"os"
	"sync/atomic"
)

func (s *Server) applyAll(patch *raftn.ApplyPatch) {
	if !raft.IsEmptySnap(patch.Snapshot) {
		<-patch.SnapSyncedC // wait raftn.RaftNode sync snapshot...
		s.applySnapshot(patch.Snapshot)
	}
	s.applyCommittedEntries(patch.CommittedEntries)
	close(patch.EntsAppliedC) // notify raftn.RaftNode

	appliedIndex, confState := s.backend.AppliedIndex(), s.backend.CurrConfState()
	s.timelineNtf.Notify(appliedIndex)

	s.tryTakeSnapshot(appliedIndex, &confState)
	if len(patch.SnapMsgs) != 0 {
		s.sendSnapshot(s.createSnapMsgs(appliedIndex, confState, patch.SnapMsgs))
	}
}

func (s *Server) applySnapshot(snap raftpb.Snapshot) {
	index, appliedIndex := snap.Metadata.Index, s.backend.AppliedIndex()
	if index <= appliedIndex {
		return
	}
	s.lg.Info(
		"applying snapshot", zap.Uint64("current-applied-index", appliedIndex),
		zap.Uint64("incoming-leader-snapshot-index", index),
		zap.Uint64("incoming-leader-snapshot-term", snap.Metadata.Term),
	)
	defer func() {
		s.lg.Info("snapshot applied", zap.Uint64("current-applied-index", s.backend.AppliedIndex()))
	}()
	dbFn, err := s.snapshotter.DBFilePath(index)
	if err != nil {
		s.lg.Panic("failed to get DB file path", zap.Uint64("snap-index", index), zap.Error(err))
	}
	f, err := os.Open(dbFn)
	if err != nil {
		s.lg.Panic("failed to open DB file", zap.String("DB-filename", dbFn), zap.Error(err))
	}
	s.lg.Info("snapshot file opened", zap.String("DB-filename", dbFn))
	s.lg.Info("start receiving snapshot")
	if err := s.backend.ReceiveSnapshot(index, f); err != nil {
		_ = f.Close()
		s.lg.Panic("backend failed to receive snapshot", zap.Error(err))
	}
	if err := f.Close(); err != nil {
		s.lg.Panic("failed to close DB file", zap.String("DB-filename", dbFn), zap.Error(err))
	}
	s.cluster.RecoverMembers()
	s.lg.Info("recovered cluster members")
	s.transport.RemoveAllPeers()
	for _, member := range s.cluster.GetPeerMembers() {
		if member.ID == s.cluster.GetLocalMember().ID {
			continue
		}
		s.transport.AddPeer(types.ID(member.ID), []string{member.AddrInfo.RaftAddress()})
	}
	s.lg.Info("recovered transport peers")
}

func (s *Server) applyCommittedEntries(ents []raftpb.Entry) {
	ents = s.entries2Apply(ents)
	if len(ents) == 0 {
		return
	}
	var shouldStop bool
	var internalReq api.InternalRequest
	var confChange = new(raftpb.ConfChange)
	for _, ent := range ents {
		if ent.Type == raftpb.EntryNormal {
			req := &internalReq
			s.lg.Debug("apply normal entry", zap.Uint64("entry-index", ent.Index))
			if len(ent.Data) == 0 {
				req = nil
			} else if err := internalReq.Unmarshal(ent.Data); err != nil {
				s.lg.Panic("failed to unmarshal internalRequest",
					zap.ByteString("data", ent.Data), zap.Error(err),
				)
			}
			s.reqNotifier.Notify(req.GetID(), s.applier.Apply(ent.Index, req))
		} else if ent.Type == raftpb.EntryConfChange {
			if err := confChange.Unmarshal(ent.Data); err != nil {
				s.lg.Panic("failed to unmarshal confChange",
					zap.ByteString("data", ent.Data), zap.Error(err),
				)
			}
			removeSelf, err := s.applyConfChange(ent.Index, confChange)
			shouldStop = shouldStop || removeSelf
			s.reqNotifier.Notify(confChange.ID, &ConfChangeResponse{err: err})
		} else {
			s.lg.Panic(
				"unknown entry type; must be either EntryNormal or EntryConfChange",
				zap.String("type", ent.Type.String()),
			)
		}
	}
	if shouldStop {
		// TODO: add stop codes...
	}
}

type ConfChangeResponse struct {
	err error
}

func (s *Server) applyConfChange(index uint64, cc *raftpb.ConfChange) (removeSelf bool, err error) {
	// TODO: implement me
	panic("not implemented yet!")
}

func (s *Server) entries2Apply(ents []raftpb.Entry) []raftpb.Entry {
	if len(ents) == 0 {
		return nil
	}
	appliedIndex := s.backend.AppliedIndex()
	if ents[0].Index > appliedIndex+1 {
		s.lg.Panic("unexpected committed entry index",
			zap.Uint64("current-applied-index", appliedIndex),
			zap.Uint64("first-entry-index", ents[0].Index),
		)
	}
	ents = ents[mathutil.MinUint64(appliedIndex+1-ents[0].Index, uint64(len(ents))):]
	return ents
}

func (s *Server) tryTakeSnapshot(index uint64, confState *raftpb.ConfState) {
	snapIndex, _ := s.memStorage.FirstIndex()
	if index-snapIndex < s.Config.SnapshotThreshold {
		return
	}
	snapshot, err := s.memStorage.CreateSnapshot(index, confState, nil)
	if err != nil {
		if err == raft.ErrSnapOutOfDate {
			return
		}
		s.lg.Panic("failed to create snapshot", zap.Uint64("index", index),
			zap.Any("confState", confState), zap.Error(err))
	}
	if err = s.walStorage.SaveSnap(snapshot); err != nil {
		s.lg.Panic("failed to save snapshot", zap.Error(err))
	}
	if err = s.walStorage.Release(snapshot); err != nil {
		s.lg.Panic("failed to release wal", zap.Error(err))
	}
	s.lg.Info("snapshot created success", zap.Uint64("snapshot-index", index),
		zap.String("cluster-name", s.Config.ClusterName),
		zap.Uint64("local-member-id", s.cluster.GetLocalMember().ID),
		zap.String("added-member-name", s.cluster.GetLocalMember().AddrInfo.Name),
	)
	if atomic.LoadInt64(&s.inflightSnapshots) > 0 {
		s.lg.Info("skip compaction since a follower is catching up")
		return
	}
	if err := s.memStorage.Compact(snapshot.Metadata.Index); err != nil {
		s.lg.Warn("failed to compact entries", zap.Error(err))
	}
}

func (s *Server) createSnapMsgs(index uint64, confState raftpb.ConfState, msgs []raftpb.Message) []snap.Message {
	ret := make([]snap.Message, 0, len(msgs))
	for _, m := range msgs {
		_, rc, size, err := s.backend.SnapshotStream()
		if err != nil {
			s.lg.Panic("failed to create snapshot bytes stream", zap.Error(err))
		}
		term, err := s.memStorage.Term(index)
		if err != nil {
			s.lg.Panic("failed to query entry's term", zap.Uint64("index", index), zap.Error(err))
		}
		m.Snapshot = raftpb.Snapshot{
			Metadata: raftpb.SnapshotMetadata{
				ConfState: confState,
				Index:     index,
				Term:      term,
			},
		}
		ret = append(ret, *snap.NewMessage(m, rc, size))
	}
	return ret
}

func (s *Server) sendSnapshot(msgs []snap.Message) {
	atomic.AddInt64(&s.inflightSnapshots, int64(len(msgs)))
	for _, msg := range msgs {
		var localMsg = msg
		s.transport.SendSnapshot(localMsg)
		s.fw.Attach(func() {
			select {
			case ok := <-localMsg.CloseNotify():
				if !ok {
					s.lg.Warn("failed to send snapshot", zap.Any("snap.Message", localMsg))
				}
				atomic.AddInt64(&s.inflightSnapshots, -1)
			case <-s.stopped:
			}
		})
	}
}
