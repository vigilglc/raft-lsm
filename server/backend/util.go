package backend

import (
	"encoding/binary"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/vigilglc/raft-lsm/server/backend/kvpb"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

func uint64ToBytes(ui uint64) []byte {
	ret := make([]byte, 8, 8)
	binary.BigEndian.PutUint64(ret, ui)
	return ret
}

func bytes2Uint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

func confState2Bytes(confState *raftpb.ConfState) (b []byte, err error) {
	return confState.Marshal()
}

func bytes2ConfState(b []byte) (confState *raftpb.ConfState, err error) {
	confState = new(raftpb.ConfState)
	err = confState.Unmarshal(b)
	return
}

func pipeIterator2Chan(fromIter iterator.Iterator, asc bool, toChan chan<- *kvpb.KV,
	errC chan<- error, closeC <-chan struct{}, stopped <-chan struct{}) {
	var size = 0
	moveNextFunc := func(iter iterator.Iterator) (hasNext bool, err error) {
		if asc {
			if size == 0 {
				hasNext = iter.First()
			} else {
				hasNext = iter.Next()
			}
		} else {
			if size == 0 {
				hasNext = iter.Last()
			} else {
				hasNext = iter.Prev()
			}
		}
		if hasNext {
			size++
		}
		return hasNext, iter.Error()
	}
	currentKVFunc := func(iter iterator.Iterator) *kvpb.KV {
		return &kvpb.KV{Key: string(iter.Key()), Val: string(iter.Value())}
	}
	var err error
	for err == nil {
		var hasNext bool
		hasNext, err = moveNextFunc(fromIter)
		if err != nil || !hasNext {
			break
		}
		select {
		case toChan <- currentKVFunc(fromIter):
		case <-closeC:
			goto FIN
		case <-stopped:
			err = leveldb.ErrClosed
		}
	}
FIN:
	if err != nil {
		errC <- err
	} else {
		close(toChan) // necessary to notify kv drained...
	}
}
