package benchmark

import (
	raftLSMBackend "github.com/vigilglc/raft-lsm/server/backend"
	etcdBackend "go.etcd.io/etcd/server/v3/mvcc/backend"
	"go.etcd.io/etcd/server/v3/mvcc/buckets"
	"testing"
)

func Benchmark_Etcd_Backend_Get(b *testing.B) {
	be, err := openEtcdBackend("etcd", false)
	if err != nil {
		b.Error(err)
	}
	defer func(be etcdBackend.Backend) {
		err := be.Close()
		if err != nil {
			b.Error(err)
		}
	}(be)
	readTx := be.ReadTx()
	b.ResetTimer()
	defer b.StopTimer()
	for i := 0; i < b.N; i++ {
		key, endKey := generateValidRange(b)
		readTx.RLock()
		readTx.UnsafeRange(buckets.Test, []byte(key), []byte(endKey), 1)
		readTx.RUnlock()
	}
}

func Benchmark_RaftLSM_Backend_Get(b *testing.B) {
	be, err := openRaftLSMBackend("raft-lsm", true, false)
	if err != nil {
		b.Error(err)
	}
	defer func(be raftLSMBackend.Backend) {
		err := be.Close()
		if err != nil {
			b.Error(err)
		}
	}(be)
	b.ResetTimer()
	defer b.StopTimer()
	for i := 0; i < b.N; i++ {
		_, _ = be.Get(generateValidKey(b))
	}
}

func generateValidRange(b *testing.B) (key, endKey string) {
	b.StopTimer()
	defer b.StartTimer()
GEN:
	bs := newRandomBytes(8)
	key = string(bs)
	if !raftLSMBackend.ValidateKey(key) {
		goto GEN
	}
	if key == string([]byte{255, 255, 255, 255, 255, 255, 255, 255}) {
		endKey = key + "!"
		return
	}
	for i := 7; i >= 0; i-- {
		if bs[i] < 255 {
			bs[i]++
			endKey = string(bs)
			break
		}
	}
	return
}
