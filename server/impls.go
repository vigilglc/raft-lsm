package server

import (
	"context"
	"github.com/gogo/protobuf/proto"
	json "github.com/json-iterator/go"
	"github.com/syndtr/goleveldb/leveldb"
	api "github.com/vigilglc/raft-lsm/server/api/rpcpb"
	"github.com/vigilglc/raft-lsm/server/backend"
	"github.com/vigilglc/raft-lsm/server/backend/kvpb"
	"github.com/vigilglc/raft-lsm/server/cluster"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/raft/v3/tracker"
	"sync/atomic"
	"time"
)

func (s *Server) doInternalRequest(ctx context.Context, req api.InternalRequest) (resp proto.Message, err error) {
	reqID := s.reqIDGen.Next()
	req.ID = reqID
	notifier := s.reqNotifier.Register(reqID)
	defer s.reqNotifier.Notify(reqID, nil)

	data, err := req.Marshal()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, s.Config.GetRequestTimeout())
	defer cancel()
	err = s.raftNode.Propose(ctx, data)
	if err != nil {
		return nil, err
	}
	select {
	case v := <-notifier:
		result := v.(*ApplyResult)
		return result.Resp, result.Err
	case <-ctx.Done():
		err = ctx.Err()
	case <-s.stopped:
		err = ErrStopped
	}
	return
}

// region kv impls

func (s *Server) Get(ctx context.Context, request *api.GetRequest) (resp *api.GetResponse, err error) {
	resp = new(api.GetResponse)
	if !backend.ValidateKey(request.Key) {
		return resp, backend.ErrInvalidKey
	}
	ctx, cancel := context.WithTimeout(ctx, s.Config.GetRequestTimeout())
	defer cancel()
	if request.Linearizable {
		if err = s.linearizableReadNotify(ctx); err != nil {
			return
		}
	}
	resp.Val, err = s.backend.Get(request.Key)
	if err != nil && err != leveldb.ErrNotFound {
		err = ErrInternalServer
	}
	return
}

func (s *Server) Put(ctx context.Context, request *api.PutRequest) (resp *api.PutResponse, err error) {
	resp = new(api.PutResponse)
	if !backend.ValidateKey(request.KeyVal.Key) {
		return resp, backend.ErrInvalidKey
	}
	msg, err := s.doInternalRequest(ctx, api.InternalRequest{Put: request})
	if err != nil {
		return resp, err
	}
	resp = msg.(*api.PutResponse)
	return
}

func (s *Server) Del(ctx context.Context, request *api.DelRequest) (resp *api.DelResponse, err error) {
	resp = new(api.DelResponse)
	if !backend.ValidateKey(request.Key) {
		return resp, backend.ErrInvalidKey
	}
	msg, err := s.doInternalRequest(ctx, api.InternalRequest{Del: request})
	if err != nil {
		return resp, err
	}
	resp = msg.(*api.DelResponse)
	return
}

func (s *Server) Write(ctx context.Context, request *api.WriteRequest) (resp *api.WriteResponse, err error) {
	resp = new(api.WriteResponse)
	for _, ent := range request.Batch {
		if !backend.ValidateKey(ent.KeyVal.Key) {
			return resp, backend.ErrInvalidKey
		}
	}
	msg, err := s.doInternalRequest(ctx, api.InternalRequest{Write: request})
	if err != nil {
		return resp, err
	}
	resp = msg.(*api.WriteResponse)
	return
}

func (s *Server) Range(agent api.KVService_RangeServer) (err error) {
	req, err := agent.Recv() // TODO: timeout check
	if err != nil {
		return err
	}
	if req.OP != api.RangeRequest_BEGIN {
		return ErrBadRequest
	}
	ctx, cancel := context.WithTimeout(agent.Context(), s.Config.GetRequestTimeout())
	defer cancel()
	if req.Linearizable {
		if err = s.linearizableReadNotify(ctx); err != nil {
			return
		}
	}
	if err = agent.Send(&api.RangeResponse{HasMore: true}); err != nil {
		return err
	}
	kvC, errC, closeC := s.backend.Range(req.From, req.To, req.Order == api.RangeRequest_ASC)
	defer close(closeC)
	for true {
		req, err = agent.Recv()
		if err != nil {
			return err
		}
		switch req.OP {
		case api.RangeRequest_NEXTN:
			kvs, err := readKVsAtMostN(kvC, errC, req.Count)
			if err != nil {
				return err
			}
			if err = agent.Send(&api.RangeResponse{KeyVals: kvs, HasMore: len(kvs) >= int(req.Count)}); err != nil {
				return err
			}
		case api.RangeRequest_CLOSE:
			err = agent.Send(&api.RangeResponse{HasMore: false})
			return err
		default:
			return ErrBadRequest
		}
	}
	return err
}

func readKVsAtMostN(kvC <-chan *kvpb.KV, errC <-chan error, n uint64) (res []*kvpb.KV, err error) {
	var done bool
	for !done && len(res) < int(n) && err == nil {
		select {
		case kv, ok := <-kvC:
			if !ok {
				done = true
				break
			}
			res = append(res, kv)
		case err = <-errC:
		}
	}
	return
}

// endregion

func (s *Server) doConfigure(ctx context.Context, confChange raftpb.ConfChange) (members []*cluster.Member, err error) {
	ccID := s.reqIDGen.Next()
	confChange.ID = ccID
	notifier := s.reqNotifier.Register(ccID)
	defer s.reqNotifier.Notify(ccID, nil)

	err = s.raftNode.ProposeConfChange(ctx, confChange)
	if err != nil {
		return nil, err
	}
	select {
	case v := <-notifier:
		result := v.(*cluster.ConfChangeResponse)
		return result.Members, result.Err
	case <-ctx.Done():
		err = ctx.Err()
	case <-s.stopped:
		err = ErrStopped
	}
	return
}

// region cluster impls

func (s *Server) LocalMember(_ context.Context, _ *api.LocalMemberRequest) (resp *api.LocalMemberResponse, err error) {
	resp = &api.LocalMemberResponse{
		Member: clusterMemberSlc2ApiMemberSlc([]*cluster.Member{s.cluster.GetLocalMember()})[0],
	}
	return resp, err
}

func (s *Server) AddMember(ctx context.Context, request *api.AddMemberRequest) (resp *api.AddMemberResponse, err error) {
	resp = new(api.AddMemberResponse)
	servPort, sOk := tryConvertUint32To16(request.Member.ServicePort)
	raftPort, rOk := tryConvertUint32To16(request.Member.RaftPort)
	if !sOk || !rOk {
		return resp, ErrBadRequest
	}
	mem := cluster.NewMember(s.cluster.GetClusterName(),
		cluster.AddrInfo{
			Name:        request.Member.Name,
			Host:        request.Member.Host,
			ServicePort: servPort,
			RaftPort:    raftPort,
		}, request.Member.IsLearner)
	ccCtx := cluster.ConfChangeContext{Member: *mem}
	data, err := json.Marshal(ccCtx)
	if err != nil {
		return resp, err
	}
	var ccType = raftpb.ConfChangeAddNode
	if mem.IsLearner {
		ccType = raftpb.ConfChangeAddLearnerNode
	}
	members, err := s.doConfigure(ctx, raftpb.ConfChange{
		Type:    ccType,
		NodeID:  mem.ID,
		Context: data,
	})
	resp.Members = clusterMemberSlc2ApiMemberSlc(members)
	return resp, err
}

func tryConvertUint32To16(n uint32) (uint16, bool) {
	if uint32(uint16(n)) == n {
		return uint16(n), true
	}
	return uint16(n), false
}

func (s *Server) PromoteMember(ctx context.Context, request *api.PromoteMemberRequest) (resp *api.PromoteMemberResponse, err error) {
	resp = new(api.PromoteMemberResponse)
	mem := &cluster.Member{ID: request.NodeID}
	ccCtx := cluster.ConfChangeContext{Member: *mem, Promote: true}
	data, err := json.Marshal(ccCtx)
	if err != nil {
		return resp, err
	}
	members, err := s.doConfigure(ctx, raftpb.ConfChange{
		Type:    raftpb.ConfChangeAddNode,
		NodeID:  mem.ID,
		Context: data,
	})
	resp.Members = clusterMemberSlc2ApiMemberSlc(members)
	return resp, err
}

func (s *Server) RemoveMember(ctx context.Context, request *api.RemoveMemberRequest) (resp *api.RemoveMemberResponse, err error) {
	resp = new(api.RemoveMemberResponse)
	mem := &cluster.Member{ID: request.NodeID}
	ccCtx := cluster.ConfChangeContext{Member: *mem}
	data, err := json.Marshal(ccCtx)
	if err != nil {
		return resp, err
	}
	members, err := s.doConfigure(ctx, raftpb.ConfChange{
		Type:    raftpb.ConfChangeRemoveNode,
		NodeID:  mem.ID,
		Context: data,
	})
	resp.Members = clusterMemberSlc2ApiMemberSlc(members)
	return resp, err
}

func (s *Server) ClusterStatus(ctx context.Context, request *api.ClusterStatusRequest) (resp *api.ClusterStatusResponse, err error) {
	resp = new(api.ClusterStatusResponse)
	ctx, cancel := context.WithTimeout(ctx, s.Config.GetRequestTimeout())
	defer cancel()
	if request.Linearizable {
		if err = s.linearizableReadNotify(ctx); err != nil {
			return
		}
	}
	cloned := s.cluster.DataClone()
	resp = &api.ClusterStatusResponse{
		ID:         cloned.GetClusterID(),
		Name:       cloned.GetClusterName(),
		Members:    clusterMemberSlc2ApiMemberSlc(cloned.GetMembers()),
		RemovedIDs: cloned.GetRemovedIDs(),
	}
	return resp, err
}

func clusterMemberSlc2ApiMemberSlc(members []*cluster.Member) []*api.Member {
	var ret = make([]*api.Member, 0, len(members))
	for _, mem := range members {
		ret = append(ret, &api.Member{
			ID:          mem.ID,
			Name:        mem.Name,
			Host:        mem.Host,
			RaftPort:    uint32(mem.RaftPort),
			ServicePort: uint32(mem.ServicePort),
			IsLearner:   mem.IsLearner,
		})
	}
	return ret
}

// endregion

// region raft impls

func (s *Server) Status(ctx context.Context, request *api.StatusRequest) (resp *api.StatusResponse, err error) {
	resp = new(api.StatusResponse)
	ctx, cancel := context.WithTimeout(ctx, s.Config.GetRequestTimeout())
	defer cancel()
	if request.Linearizable {
		if err = s.linearizableReadNotify(ctx); err != nil {
			return
		}
	}
	status := s.raftNode.Status()
	resp = raftStatus2ApiStatus(&status)
	return
}

var raftStateApiStateMapping = map[raft.StateType]api.RaftState{
	raft.StateFollower:     api.RaftState_Follower,
	raft.StateCandidate:    api.RaftState_Candidate,
	raft.StateLeader:       api.RaftState_Leader,
	raft.StatePreCandidate: api.RaftState_PreCandidate,
}

var progressStateApiStateMapping = map[tracker.StateType]api.ProgressState{
	tracker.StateProbe:     api.ProgressState_Probe,
	tracker.StateReplicate: api.ProgressState_Replicate,
	tracker.StateSnapshot:  api.ProgressState_Snapshot,
}

func raftStatus2ApiStatus(status *raft.Status) *api.StatusResponse {
	resp := &api.StatusResponse{
		NodeID:         status.ID,
		RaftState:      raftStateApiStateMapping[status.RaftState],
		Term:           status.HardState.Term,
		Lead:           status.SoftState.Lead,
		AppliedIndex:   status.Applied,
		CommittedIndex: status.Commit,
	}
	if status.Progress != nil {
		for nodeID, prg := range status.Progress {
			resp.Progresses = append(resp.Progresses, &api.RaftProgress{
				NodeID:       nodeID,
				MatchIndex:   prg.Match,
				NextIndex:    prg.Next,
				State:        progressStateApiStateMapping[prg.State],
				RecentActive: prg.RecentActive,
				IsLearner:    prg.IsLearner,
			})
		}
	}
	return resp
}

func (s *Server) TransferLeader(ctx context.Context, request *api.TransferLeaderRequest) (resp *api.TransferLeaderResponse, err error) {
	resp = new(api.TransferLeaderResponse)
	transferee := request.TransfereeID
	if transferee == raft.None {
		return resp, ErrInvalidArgs
	}
	if s.cluster.IsIDRemoved(transferee) {
		return resp, ErrInvalidArgs
	}
	ctx, cancel := context.WithTimeout(ctx, s.Config.GetRequestTimeout())
	defer cancel()

	s.raftNode.TransferLeadership(ctx, atomic.LoadUint64(&s.lead), transferee)
	var interval = s.Config.GetOneTickMillis()
	leaderNotifier := s.leaderChangeNtf.Wait()
	for atomic.LoadUint64(&s.lead) != transferee {
		select {
		case <-time.After(interval):
		case <-leaderNotifier:
			leaderNotifier = s.leaderChangeNtf.Wait()
		case <-ctx.Done():
			err = ctx.Err()
			break
		}
	}
	return resp, err
}

// endregion
