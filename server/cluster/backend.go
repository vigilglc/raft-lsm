package cluster

import (
	"fmt"
	json "github.com/json-iterator/go"
	"github.com/vigilglc/raft-lsm/server/backend"
	"github.com/vigilglc/raft-lsm/server/backend/kvpb"
	"github.com/vigilglc/raft-lsm/server/utils/syncutil"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"strconv"
)

const (
	memberKeyPrefix          = backend.ReservedSPrefix + "MEMBER"
	removedMemberIDKeyPrefix = backend.ReservedSPrefix + "REMOVED_MEMBER_ID"
)

const (
	memberKeyRangeFrom = memberKeyPrefix + "-"
	memberKeyRangeTo   = memberKeyPrefix + "."

	removedMemberIDKeyRangeFrom = removedMemberIDKeyPrefix + "-"
	removedMemberIDKeyRangeTo   = removedMemberIDKeyPrefix + "."
)

func memberKey(ID uint64) string {
	return fmt.Sprintf("%s-%d", memberKeyPrefix, ID)
}

func removedMemberIDKey(ID uint64) string {
	return fmt.Sprintf("%s-%d", removedMemberIDKeyPrefix, ID)
}

func (cl *Cluster) PushMembers2Backend() error {
	defer syncutil.SchedLockers(&cl.rwmu)()
	builder := backend.NewBatchBuilder()
	for _, mem := range cl.members {
		memJst, err := json.MarshalToString(mem)
		if err != nil {
			return err
		}
		builder = builder.Put(memberKey(mem.ID), memJst)
	}
	return cl.be.Write(builder.Finish())
}

func (cl *Cluster) addMember2Backend(ai uint64, confState *raftpb.ConfState, mem *Member) error {
	memJst, err := json.MarshalToString(mem)
	if err != nil {
		return err
	}
	return cl.be.PutConfState(ai, *confState, &kvpb.KV{
		Key: memberKey(mem.ID), Val: memJst,
	})
}

func (cl *Cluster) removeMember2Backend(ai uint64, confState *raftpb.ConfState, ID uint64) error {
	err := cl.be.PutConfState(ai, *confState, []*kvpb.KV{
		{Key: memberKey(ID), Val: strconv.FormatUint(ID, 10)},
		{Key: removedMemberIDKey(ID), Val: ""},
	}...)
	if err == nil {
		err = cl.be.Del(ai, memberKey(ID))
	}
	return err
}

func (cl *Cluster) promoteMember2Backend(ai uint64, confState *raftpb.ConfState, ID uint64) error {
	memJst, err := cl.be.Get(memberKey(ID))
	if err != nil {
		return err
	}
	mem := new(Member)
	err = json.UnmarshalFromString(memJst, mem)
	if err != nil {
		return err
	}
	mem.IsLearner = true
	return cl.addMember2Backend(ai, confState, mem)
}

func (cl *Cluster) membersFromBackend() (members map[uint64]*Member, err error) {
	kvC, errC, closeC := cl.be.Range(memberKeyRangeFrom, memberKeyRangeTo, true)
	defer close(closeC)
	members = map[uint64]*Member{}
	for err == nil {
		select {
		case err = <-errC:
		case kv := <-kvC:
			if len(kv.Val) == 0 {
				continue
			}
			mem := new(Member)
			err = json.UnmarshalFromString(kv.Val, mem)
			members[mem.ID] = mem
		}
	}
	return
}

func (cl *Cluster) removedIDsFromBackend() (removedIDs map[uint64]struct{}, err error) {
	kvC, errC, closeC := cl.be.Range(removedMemberIDKeyRangeFrom, removedMemberIDKeyRangeTo, true)
	defer close(closeC)
	removedIDs = map[uint64]struct{}{}
	for err == nil {
		select {
		case err = <-errC:
		case kv := <-kvC:
			if len(kv.Val) == 0 {
				continue
			}
			ID, err := strconv.ParseUint(kv.Val, 10, 64)
			if err != nil {
				return nil, err
			}
			removedIDs[ID] = struct{}{}
		}
	}
	return
}
