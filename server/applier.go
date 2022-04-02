package server

import (
	"github.com/gogo/protobuf/proto"
	api "github.com/vigilglc/raft-lsm/server/api/rpcpb"
	"github.com/vigilglc/raft-lsm/server/backend"
	"go.uber.org/zap"
)

type ApplyResult struct {
	Resp proto.Message
	Err  error
}

type Applier interface {
	Apply(index uint64, req *api.InternalRequest) *ApplyResult
	Put(index uint64, req *api.PutRequest) (resp *api.PutResponse, err error)
	Del(index uint64, req *api.DelRequest) (resp *api.DelResponse, err error)
	Write(index uint64, req *api.WriteRequest) (resp *api.WriteResponse, err error)
}

type backendApplier struct {
	lg *zap.Logger
	be backend.Backend
}

func NewBackendApplier(lg *zap.Logger, be backend.Backend) Applier {
	return &backendApplier{lg: lg, be: be}
}

func (ba *backendApplier) Apply(index uint64, req *api.InternalRequest) (ret *ApplyResult) {
	ret = &ApplyResult{Resp: nil, Err: nil}
	switch {
	case req == nil:
		ret.Resp, ret.Err = ba.Write(index, &api.WriteRequest{Batch: nil})
	case req.Put != nil:
		ret.Resp, ret.Err = ba.Put(index, req.Put)
	case req.Del != nil:
		ret.Resp, ret.Err = ba.Del(index, req.Del)
	case req.Write != nil:
		ret.Resp, ret.Err = ba.Write(index, req.Write)
	}
	return ret
}

func (ba *backendApplier) Put(index uint64, req *api.PutRequest) (resp *api.PutResponse, err error) {
	err = ba.be.Put(index, req.KeyVal.Key, req.KeyVal.Val)
	return
}

func (ba *backendApplier) Del(index uint64, req *api.DelRequest) (resp *api.DelResponse, err error) {
	err = ba.be.Del(index, req.Key)
	return
}

func (ba *backendApplier) Write(index uint64, req *api.WriteRequest) (resp *api.WriteResponse, err error) {
	builder := backend.NewBatchBuilder().AppliedIndex(index)
	for _, e := range req.Batch {
		if e.OP == api.BatchEntry_PUT {
			builder = builder.Put(e.KeyVal.Key, e.KeyVal.Val)
		} else if e.OP == api.BatchEntry_DEL {
			builder = builder.Del(e.KeyVal.Key)
		}
	}
	err = ba.be.Write(builder.Finish())
	return
}
