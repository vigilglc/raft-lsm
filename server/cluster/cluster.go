package cluster

import (
	"crypto/sha1"
	"encoding/binary"
	json "github.com/json-iterator/go"
	"github.com/vigilglc/raft-lsm/server/backend"
	"github.com/vigilglc/raft-lsm/server/utils/syncutil"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"
	"sort"
	"sync"
)

type Cluster struct {
	lg   *zap.Logger
	name string
	id   uint64

	rwmu       sync.RWMutex
	localMemID uint64
	members    map[uint64]*Member
	removedIDs map[uint64]struct{}
	be         backend.Backend
}

func (cl *Cluster) DataClone() *Cluster {
	defer syncutil.SchedLockers(cl.rwmu.RLocker())()
	ret := &Cluster{
		name:       cl.name,
		id:         cl.id,
		localMemID: cl.localMemID,
		members:    map[uint64]*Member{},
		removedIDs: map[uint64]struct{}{},
	}
	for _, mem := range cl.members {
		ret.members[mem.ID] = mem.Clone()
	}
	for remID := range cl.removedIDs {
		ret.removedIDs[remID] = struct{}{}
	}
	return ret
}

func (cl *Cluster) GetClusterID() uint64 {
	defer syncutil.SchedLockers(cl.rwmu.RLocker())()
	return cl.id
}

func (cl *Cluster) GetClusterName() string {
	defer syncutil.SchedLockers(cl.rwmu.RLocker())()
	return cl.name
}

func (cl *Cluster) SetClusterName(cname string) {
	defer syncutil.SchedLockers(&cl.rwmu)()
	cl.name = cname
}

func (cl *Cluster) SetClusterID(ID uint64) {
	defer syncutil.SchedLockers(&cl.rwmu)()
	cl.id = ID
}

func (cl *Cluster) GetLocalMemberID() uint64 {
	defer syncutil.SchedLockers(cl.rwmu.RLocker())()
	return cl.localMemID
}

func (cl *Cluster) GetLocalMember() *Member {
	defer syncutil.SchedLockers(cl.rwmu.RLocker())()
	return cl.members[cl.localMemID].Clone()
}

func (cl *Cluster) GetMembers() []*Member {
	defer syncutil.SchedLockers(cl.rwmu.RLocker())()
	var ret []*Member
	for _, mem := range cl.members {
		ret = append(ret, mem.Clone())
	}
	return ret
}

func (cl *Cluster) GetRemovedIDs() []uint64 {
	defer syncutil.SchedLockers(cl.rwmu.RLocker())()
	var ret []uint64
	for ID, _ := range cl.removedIDs {
		ret = append(ret, ID)
	}
	return ret
}

func (cl *Cluster) IsIDRemoved(ID uint64) bool {
	defer syncutil.SchedLockers(cl.rwmu.RLocker())()
	_, ok := cl.removedIDs[ID]
	return ok
}

func (cl *Cluster) SetBackend(be backend.Backend) {
	cl.be = be
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
	mem = mem.Clone()
	defer syncutil.SchedLockers(&cl.rwmu)()
	cl.members[mem.ID] = mem
	if err := cl.addMember2Backend(ai, confState, mem); err != nil {
		cl.lg.Fatal("failed to put AddMember changes to backend", zap.Error(err))
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
	cl.removedIDs[ID] = struct{}{}
	if cl.be != nil {
		if err := cl.removeMember2Backend(ai, confState, ID); err != nil {
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
	cl.members[ID].IsLearner = false
	if err := cl.promoteMember2Backend(ai, confState, ID); err != nil {
		cl.lg.Fatal("failed to put PromoteMember changes to backend", zap.Error(err))
	}
	cl.lg.Info("add member success",
		zap.String("cluster-name", cl.name),
		zap.Uint64("local-member-id", cl.localMemID),
		zap.Uint64("promoted-member-id", ID),
	)
}

func (cl *Cluster) RecoverMembers() error {
	defer syncutil.SchedLockers(&cl.rwmu)()
	members, err := cl.membersFromBackend()
	if err != nil {
		cl.lg.Error("failed to recover members from backend", zap.Error(err))
		return err
	}

	removedIDs, err := cl.removedIDsFromBackend()
	if err != nil {
		cl.lg.Error("failed to recover removed member IDs from backend", zap.Error(err))
		return err
	}
	localMem := cl.members[cl.localMemID]
	cl.members, cl.removedIDs = members, removedIDs
	if _, ok := cl.members[localMem.ID]; !ok {
		cl.members[localMem.ID] = localMem
	}
	cl.lg.Info("recover members success",
		zap.String("cluster-name", cl.name),
		zap.Uint64("local-member-id", cl.localMemID),
	)
	return nil
}

type ConfChangeContext struct {
	Member
	Promote bool `json:"Promote,omitempty"`
}
type ConfChangeResponse struct {
	Members []*Member
	Err     error
}

func (cl *Cluster) ValidateConfChange(ai uint64, cc *raftpb.ConfChange) (ccCtx *ConfChangeContext, err error) {
	if ai <= cl.be.AppliedIndex() {
		return nil, ErrAlreayApplied
	}
	defer syncutil.SchedLockers(cl.rwmu.RLocker())()
	members, err := cl.membersFromBackend()
	if err != nil {
		cl.lg.Panic("failed to recover members from backend", zap.Error(err))
	}
	removedIDs, err := cl.removedIDsFromBackend()
	if err != nil {
		cl.lg.Panic("failed to recover removed member IDs from backend", zap.Error(err))
	}

	if _, ok := removedIDs[cc.NodeID]; ok {
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
			if _, ok := members[nodeID]; !ok {
				return ccCtx, ErrIDNotExists
			}
			if !members[nodeID].IsLearner {
				return ccCtx, ErrNotLearner
			}
		} else {
			if _, ok := members[nodeID]; ok {
				return ccCtx, ErrIDExists
			}
			var addresses = map[string]struct{}{}
			for _, mem := range members {
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
		if _, ok := members[nodeID]; !ok {
			return ccCtx, ErrIDNotExists
		}
	}
	return
}

type Builder struct {
	cl  *Cluster
	err error
}

func NewClusterBuilder(lg *zap.Logger, name string, backend backend.Backend) *Builder {
	return &Builder{
		cl: &Cluster{
			lg:         lg,
			name:       name,
			members:    map[uint64]*Member{},
			removedIDs: map[uint64]struct{}{},
			be:         backend,
		},
	}
}

func (b *Builder) SetLocalMember(ID uint64) *Builder {
	b.cl.localMemID = ID
	return b
}

func (b *Builder) AddMember(addrInfo AddrInfo) *Builder {
	cl := b.cl
	mem := NewMember(cl.name, addrInfo, false)
	if _, ok := cl.members[mem.ID]; ok {
		if b.err != nil {
			b.err = ErrIDExists
		}
	}
	cl.members[mem.ID] = mem
	return b
}

func (b *Builder) AddMembers(addrInfos ...AddrInfo) *Builder {
	for _, aif := range addrInfos {
		b = b.AddMember(aif)
	}
	return b
}

func computeClusterID(members []*Member) uint64 {
	sort.Slice(members, func(i, j int) bool {
		return members[i].ID < members[j].ID
	})
	var data = make([]byte, 0, len(members)*8)
	var buf = make([]byte, 8)
	for _, mem := range members {
		binary.BigEndian.PutUint64(buf, mem.ID)
		data = append(data, buf...)
	}
	sum := sha1.Sum(data)
	return binary.BigEndian.Uint64(sum[:8])
}

func (b *Builder) Finish() (*Cluster, error) {
	if b.err != nil {
		return nil, b.err
	}
	var addresses = map[string]struct{}{}
	for _, mem := range b.cl.members {
		if _, ok := addresses[mem.RaftAddress()]; ok {
			return nil, ErrAddressClash
		}
		if _, ok := addresses[mem.ServiceAddress()]; ok {
			return nil, ErrAddressClash
		}
	}
	if b.cl.localMemID == raft.None {
		return nil, ErrNoLocalID
	}
	b.cl.SetClusterID(computeClusterID(b.cl.GetMembers()))
	return b.cl, nil
}
