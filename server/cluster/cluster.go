package cluster

import (
	json "github.com/json-iterator/go"
	"github.com/vigilglc/raft-lsm/server/backend"
	"github.com/vigilglc/raft-lsm/server/backend/kvpb"
	"github.com/vigilglc/raft-lsm/server/utils/syncutil"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"
	"sync"
)

type Cluster struct {
	lg          *zap.Logger
	name        string
	localMember *Member

	rwmu             sync.RWMutex
	peerMembers      map[uint64]*Member
	removedMemberIDs map[uint64]struct{}
	be               backend.Backend
}

const (
	peerMembersKey      = backend.ReservedSPrefix + "PEER_MEMBERS"
	removedMemberIDsKey = backend.ReservedSPrefix + "REMOVED_MEMBER_IDS"
)

func (cl *Cluster) GetLocalMember() *Member {
	defer syncutil.SchedLockers(cl.rwmu.RLocker())()
	return cl.localMember
}

func (cl *Cluster) GetPeerMembers() []*Member {
	defer syncutil.SchedLockers(cl.rwmu.RLocker())()
	var ret []*Member
	for _, mem := range cl.peerMembers {
		ret = append(ret, mem)
	}
	return ret
}

func (cl *Cluster) SetBackend(be backend.Backend) {
	cl.be = be
}

func encodePeerMembers(peerMembers map[uint64]*Member) (string, error) {
	return json.MarshalToString(peerMembers)
}
func decodePeerMembers(data string) (peerMembers map[uint64]*Member, err error) {
	peerMembers = map[uint64]*Member{}
	err = json.UnmarshalFromString(data, &peerMembers)
	return
}
func encodeRemovedMemberIDs(removedMemberIDs map[uint64]struct{}) (string, error) {
	return json.MarshalToString(removedMemberIDs)
}

func decodeRemovedMemberIDs(data string) (removedMemberIDs map[uint64]struct{}, err error) {
	removedMemberIDs = map[uint64]struct{}{}
	err = json.UnmarshalFromString(data, &removedMemberIDs)
	return
}

func (cl *Cluster) AddMember(ai uint64, confState *raftpb.ConfState, mem *Member) {
	defer syncutil.SchedLockers(&cl.rwmu)()
	cl.peerMembers[mem.ID] = mem
	memJstr, err := encodePeerMembers(cl.peerMembers)
	if err != nil {
		cl.lg.Fatal("failed to encode cluster peerMembers",
			zap.Any("peerMembers", cl.peerMembers), zap.Error(err),
		)
	}
	if cl.be != nil {
		if err := cl.be.PutConfState(ai, *confState, &kvpb.KV{Key: peerMembersKey, Val: memJstr}); err != nil {
			cl.lg.Fatal("failed to put AddMember changes to backend", zap.Error(err))
		}
	}
	cl.lg.Info("add member success",
		zap.String("cluster-name", cl.name),
		zap.Uint64("local-member-id", cl.localMember.ID),
		zap.String("added-member-name", mem.AddrInfo.Name),
		zap.Uint64("added-member-id", mem.ID),
	)
}

func (cl *Cluster) RemoveMember(ai uint64, confState *raftpb.ConfState, ID uint64) {
	defer syncutil.SchedLockers(&cl.rwmu)()
	delete(cl.peerMembers, ID)
	cl.removedMemberIDs[ID] = struct{}{}
	memJstr, err := encodePeerMembers(cl.peerMembers)
	if err != nil {
		cl.lg.Fatal("failed to encode cluster peerMembers",
			zap.Any("peerMembers", cl.peerMembers), zap.Error(err),
		)
	}
	remJstr, err := encodeRemovedMemberIDs(cl.removedMemberIDs)
	if err != nil {
		cl.lg.Fatal("failed to encode cluster removedMemberIDs",
			zap.Any("removedMemberIDs", cl.removedMemberIDs), zap.Error(err),
		)
	}
	if cl.be != nil {
		if err := cl.be.PutConfState(ai, *confState,
			&kvpb.KV{Key: peerMembersKey, Val: memJstr},
			&kvpb.KV{Key: removedMemberIDsKey, Val: remJstr},
		); err != nil {
			cl.lg.Fatal("failed to put RemoveMember changes to backend", zap.Error(err))
		}
	}
	cl.lg.Info("remove member success",
		zap.String("cluster-name", cl.name),
		zap.Uint64("local-member-id", cl.localMember.ID),
		zap.Uint64("removed-member-id", ID),
	)
}

func (cl *Cluster) PromoteMember(ai uint64, confState *raftpb.ConfState, ID uint64) {
	defer syncutil.SchedLockers(&cl.rwmu)()
	cl.peerMembers[ID].IsLearner = true
	memJstr, err := encodePeerMembers(cl.peerMembers)
	if err != nil {
		cl.lg.Fatal("failed to encode cluster peerMembers",
			zap.Any("peerMembers", cl.peerMembers), zap.Error(err),
		)
	}
	if cl.be != nil {
		if err := cl.be.PutConfState(ai, *confState, &kvpb.KV{Key: peerMembersKey, Val: memJstr}); err != nil {
			cl.lg.Fatal("failed to put PromoteMember changes to backend", zap.Error(err))
		}
	}
	cl.lg.Info("add member success",
		zap.String("cluster-name", cl.name),
		zap.Uint64("local-member-id", cl.localMember.ID),
		zap.Uint64("promoted-member-id", ID),
	)
}

func (cl *Cluster) RecoverMembers() {
	defer syncutil.SchedLockers(&cl.rwmu)()
	memJstr, err := cl.be.Get(peerMembersKey)
	if err != nil {
		cl.lg.Panic("failed to get cluster peerMembers from Backend", zap.Error(err))
	}
	cl.peerMembers, err = decodePeerMembers(memJstr)
	if err != nil {
		cl.lg.Panic("failed to decode cluster peerMembers", zap.Error(err))
	}
	remJstr, err := cl.be.Get(removedMemberIDsKey)
	if err != nil {
		cl.lg.Panic("failed to get cluster removedMemberIDs from Backend", zap.Error(err))
	}
	cl.removedMemberIDs, err = decodeRemovedMemberIDs(remJstr)
	if err != nil {
		cl.lg.Panic("failed to decode cluster removedMemberIDs", zap.Error(err))
	}
	cl.lg.Info("remove member success",
		zap.String("cluster-name", cl.name),
		zap.Uint64("local-member-id", cl.localMember.ID),
	)
}