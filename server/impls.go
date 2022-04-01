package server

import (
	"context"
	"github.com/gogo/protobuf/proto"
	"github.com/vigilglc/raft-lsm/server/api"
	"github.com/vigilglc/raft-lsm/server/backend"
	"github.com/vigilglc/raft-lsm/server/backend/kvpb"
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
	if backend.ValidateKey(request.Key) {
		return resp, backend.ErrInvalidKey
	}
	if request.Linearizable {
		if err = s.linearizableReadNotify(ctx); err != nil {
			return
		}
	}
	resp.Val, err = s.backend.Get(request.Key)
	if err != nil {
		err = ErrInternalServer
	}
	return
}

func (s *Server) Put(ctx context.Context, request *api.PutRequest) (resp *api.PutResponse, err error) {
	resp = new(api.PutResponse)
	if backend.ValidateKey(request.KeyVal.Key) {
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
	if backend.ValidateKey(request.Key) {
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
		if backend.ValidateKey(ent.KeyVal.Key) {
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
	for len(res) < int(n) && err == nil {
		select {
		case kv, ok := <-kvC:
			if !ok {
				break
			}
			res = append(res, kv)
		case err = <-errC:
		}
	}
	return
}

// endregion
