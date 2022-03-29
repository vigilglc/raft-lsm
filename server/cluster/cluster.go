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
	lg   *zap.Logger
	name string

	rwmu             sync.RWMutex
	localMemID       uint64
	members          map[uint64]*Member
	removedMemberIDs map[uint64]struct{}
	be               backend.Backend
}

const (
	membersKey          = backend.ReservedSPrefix + "MEMBERS"
	removedMemberIDsKey = backend.ReservedSPrefix + "REMOVED_MEMBER_IDS"
)

func (cl *Cluster) GetLocalMember() *Member {
	defer syncutil.SchedLockers(cl.rwmu.RLocker())()
	return cl.members[cl.localMemID]
}

func (cl *Cluster) GetMembers() []*Member {
	defer syncutil.SchedLockers(cl.rwmu.RLocker())()
	var ret []*Member
	for _, mem := range cl.members {
		ret = append(ret, mem)
	}
	return ret
}

func (cl *Cluster) SetBackend(be backend.Backend) {
	cl.be = be
}

func encodeMembers(peerMembers map[uint64]*Member) (string, error) {
	return json.MarshalToString(peerMembers)
}
func decodeMembers(data string) (peerMembers map[uint64]*Member, err error) {
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

func (cl *Cluster) SkipConfState(ai uint64) {
	defer syncutil.SchedLockers(&cl.rwmu)()
	if cl.be != nil {
		if err := cl.be.Write(backend.NewBatchBuilder().AppliedIndex(ai).Finish()); err != nil {
			cl.lg.Fatal("failed to update appliedIndex to backend", zap.Uint64("applied-index", ai), zap.Error(err))
		}
	}
}

func (cl *Cluster) AddMember(ai uint64, confState *raftpb.ConfState, mem *Member) {
	defer syncutil.SchedLockers(&cl.rwmu)()
	cl.members[mem.ID] = mem
	memJstr, err := encodeMembers(cl.members)
	if err != nil {
		cl.lg.Fatal("failed to encode cluster peerMembers",
			zap.Any("peerMembers", cl.members), zap.Error(err),
		)
	}
	if cl.be != nil {
		if err := cl.be.PutConfState(ai, *confState, &kvpb.KV{Key: membersKey, Val: memJstr}); err != nil {
			cl.lg.Fatal("failed to put AddMember changes to backend", zap.Error(err))
		}
	}
	cl.lg.Info("add member success",
		zap.String("cluster-name", cl.name),
		zap.Uint64("local-member-id", cl.localMemID),
		zap.String("added-member-name", mem.AddrInfo.Name),
		zap.Uint64("added-member-id", mem.ID),
	)
}

func (cl *Cluster) RemoveMember(ai uint64, confState *raftpb.ConfState, ID uint64) {
	defer syncutil.SchedLockers(&cl.rwmu)()
	delete(cl.members, ID)
	cl.removedMemberIDs[ID] = struct{}{}
	memJstr, err := encodeMembers(cl.members)
	if err != nil {
		cl.lg.Fatal("failed to encode cluster peerMembers",
			zap.Any("peerMembers", cl.members), zap.Error(err),
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
			&kvpb.KV{Key: membersKey, Val: memJstr},
			&kvpb.KV{Key: removedMemberIDsKey, Val: remJstr},
		); err != nil {
			cl.lg.Fatal("failed to put RemoveMember changes to backend", zap.Error(err))
		}
	}
	cl.lg.Info("remove member success",
		zap.String("cluster-name", cl.name),
		zap.Uint64("local-member-id", cl.localMemID),
		zap.Uint64("removed-member-id", ID),
	)
}

func (cl *Cluster) PromoteMember(ai uint64, confState *raftpb.ConfState, ID uint64) {
	defer syncutil.SchedLockers(&cl.rwmu)()
	cl.members[ID].IsLearner = true
	memJstr, err := encodeMembers(cl.members)
	if err != nil {
		cl.lg.Fatal("failed to encode cluster peerMembers",
			zap.Any("peerMembers", cl.members), zap.Error(err),
		)
	}
	if cl.be != nil {
		if err := cl.be.PutConfState(ai, *confState, &kvpb.KV{Key: membersKey, Val: memJstr}); err != nil {
			cl.lg.Fatal("failed to put PromoteMember changes to backend", zap.Error(err))
		}
	}
	cl.lg.Info("add member success",
		zap.String("cluster-name", cl.name),
		zap.Uint64("local-member-id", cl.localMemID),
		zap.Uint64("promoted-member-id", ID),
	)
}

func (cl *Cluster) RecoverMembers() {
	defer syncutil.SchedLockers(&cl.rwmu)()
	memJstr, err := cl.be.Get(membersKey)
	if err != nil {
		cl.lg.Panic("failed to get cluster peerMembers from Backend", zap.Error(err))
	}
	cl.members, err = decodeMembers(memJstr)
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
		zap.Uint64("local-member-id", cl.localMemID),
	)
}

type ConfChangeContext struct {
	Member
	Promote bool `json:"Promote"`
}
type ConfChangeResponse struct {
	Members []*Member
	Err     error
}

func (cl *Cluster) ValidateConfChange(cc *raftpb.ConfChange) (ccCtx *ConfChangeContext, err error) {
	defer syncutil.SchedLockers(cl.rwmu.RLocker())()
	if _, ok := cl.removedMemberIDs[cc.NodeID]; ok {
		return nil, ErrIDRemoved
	}
	ccCtx = new(ConfChangeContext)
	if err := json.Unmarshal(cc.Context, ccCtx); err != nil {
		cl.lg.Panic("failed to unmarshal confChangeContext", zap.Error(err))
	}
	nodeID := cc.NodeID
	if ccCtx.Member.ID != nodeID {
		cl.lg.Panic("got malformed confChange", zap.Uint64("confChange-nodeID", nodeID),
			zap.Uint64("confChangeContext-Member-ID", ccCtx.Member.ID))
	}
	switch cc.Type {
	case raftpb.ConfChangeAddNode, raftpb.ConfChangeAddLearnerNode:
		if ccCtx.Promote {
			if _, ok := cl.members[nodeID]; !ok {
				return ccCtx, ErrIDRemoved
			}
			if !cl.members[nodeID].IsLearner {
				return ccCtx, ErrNotLearner
			}
		} else {
			if _, ok := cl.members[nodeID]; ok {
				return ccCtx, ErrIDExists
			}
			var addresses = map[string]struct{}{}
			for _, mem := range cl.members {
				addresses[mem.RaftAddress()] = struct{}{}
				addresses[mem.ServiceAddress()] = struct{}{}
			}
			for addr, _ := range addresses {
				if addr == ccCtx.Member.RaftAddress() || addr == ccCtx.Member.ServiceAddress() {
					return ccCtx, ErrAddressClash
				}
			}
		}
	case raftpb.ConfChangeRemoveNode:
		if _, ok := cl.members[nodeID]; !ok {
			return ccCtx, ErrIDNotExists
		}
	}
	return
}
