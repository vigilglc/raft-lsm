package main

import (
	"github.com/vigilglc/raft-lsm/client/app"
	"github.com/vigilglc/raft-lsm/server/api/rpcpb"
	"github.com/vigilglc/raft-lsm/server/backend/kvpb"
)

type rangeExecutor struct {
	rgCli   rpcpb.KVService_RangeClient
	release func()
}

func NewRangeExecutor(rgCli rpcpb.KVService_RangeClient, release func()) (exe app.RangeExecutor, err error) {
	exe = &rangeExecutor{rgCli: rgCli, release: release}
	return exe, nil
}

func (re *rangeExecutor) Begin(from, to string, asc, linearizable bool) (err error) {
	order := rpcpb.RangeRequest_ASC
	if !asc {
		order = rpcpb.RangeRequest_DESC
	}
	err = re.rgCli.Send(&rpcpb.RangeRequest{
		OP:   rpcpb.RangeRequest_BEGIN,
		From: from, To: to,
		Order:        order,
		Linearizable: linearizable,
	})
	if err != nil {
		return
	}
	_, err = re.rgCli.Recv()
	return
}

func (re *rangeExecutor) Next(N uint64) (kvs []*kvpb.KV, hasMore bool, err error) {
	err = re.rgCli.Send(&rpcpb.RangeRequest{
		OP:    rpcpb.RangeRequest_NEXTN,
		Count: N,
	})
	if err != nil {
		return
	}
	resp, err := re.rgCli.Recv()
	if resp != nil {
		kvs = resp.KeyVals
		hasMore = resp.HasMore
	}
	return
}

func (re *rangeExecutor) Close() (err error) {
	defer func() {
		if err == nil {
			err = re.rgCli.CloseSend()
		}
		re.release()
	}()
	err = re.rgCli.Send(&rpcpb.RangeRequest{
		OP: rpcpb.RangeRequest_CLOSE,
	})
	_, err = re.rgCli.Recv()
	return
}
