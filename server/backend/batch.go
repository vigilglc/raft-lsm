package backend

import (
	"go.etcd.io/etcd/raft/v3/raftpb"
	"sync"
)

type batchEntry struct {
	key string
	val *string
}

type Batch struct {
	appliedIndex uint64
	confState    *raftpb.ConfState
	ents         []batchEntry
}

type BatchBuilder struct {
	mu    sync.Mutex
	batch *Batch
}

func NewBatchBuilder() *BatchBuilder {
	return &BatchBuilder{sync.Mutex{}, &Batch{}}
}

func (bb *BatchBuilder) Put(key, val string) *BatchBuilder {
	batch := bb.batch
	cpv := val
	bb.mu.Lock()
	defer bb.mu.Unlock()
	batch.ents = append(batch.ents, batchEntry{key: key, val: &cpv})
	return bb
}

func (bb *BatchBuilder) Del(key string) *BatchBuilder {
	batch := bb.batch
	bb.mu.Lock()
	defer bb.mu.Unlock()
	batch.ents = append(batch.ents, batchEntry{key: key, val: nil})
	return bb
}

func (bb *BatchBuilder) AppliedIndex(ai uint64) *BatchBuilder {
	bb.mu.Lock()
	defer bb.mu.Unlock()
	bb.batch.appliedIndex = ai
	return bb
}

func (bb *BatchBuilder) confState(confState *raftpb.ConfState) *BatchBuilder {
	bb.mu.Lock()
	defer bb.mu.Unlock()
	bb.batch.confState = confState
	return bb
}

func (bb *BatchBuilder) Finish() *Batch {
	batch := bb.batch
	bb.mu.Lock()
	defer bb.mu.Unlock()
	kvM := map[string]*string{}
	for _, ent := range batch.ents {
		kvM[ent.key] = ent.val
	}
	batch.ents = batch.ents[:0]
	for k, v := range kvM {
		batch.ents = append(batch.ents, batchEntry{k, v})
	}
	cpb := *batch
	return &cpb
}
