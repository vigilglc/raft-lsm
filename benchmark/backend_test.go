package benchmark

import (
	raftLSMBackend "github.com/vigilglc/raft-lsm/server/backend"
	etcdBackend "go.etcd.io/etcd/server/v3/mvcc/backend"
	"go.etcd.io/etcd/server/v3/mvcc/buckets"
	"go.uber.org/zap"
	"os"
	"path/filepath"
	"testing"
)

func openEtcdBackend(name string, clean bool) (be etcdBackend.Backend, err error) {
	gopath, set := os.LookupEnv("GOPATH")
	if !set {
		os.Exit(-1)
	}
	dbDir := filepath.Join(gopath, "temp", "benchmark", "backend", name)
	if clean {
		_ = os.RemoveAll(dbDir)
	}
	if err := os.MkdirAll(dbDir, 0666); err != nil {
		os.Exit(-1)
	}
	config := etcdBackend.DefaultBackendConfig()
	config.Path = filepath.Join(dbDir, "bolt.db")
	config.Logger = zap.NewExample()
	return etcdBackend.New(config), nil
}

func openRaftLSMBackend(name string, sync bool, clean bool) (be raftLSMBackend.Backend, err error) {
	gopath, set := os.LookupEnv("GOPATH")
	if !set {
		os.Exit(-1)
	}
	dbDir := filepath.Join(gopath, "temp", "benchmark", "backend", name)
	if clean {
		_ = os.RemoveAll(dbDir)
	}
	if err := os.MkdirAll(dbDir, 0666); err != nil {
		os.Exit(-1)
	}
	config := raftLSMBackend.Config{
		Dir:        dbDir,
		ForceClose: false,
		Sync:       sync,
	}
	return raftLSMBackend.OpenBackend(zap.NewExample(), config)
}

func Benchmark_Etcd_Backend_Put(b *testing.B) {
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
	batchTx := be.BatchTx()

	batchTx.Lock()
	batchTx.UnsafeCreateBucket(buckets.Test)
	batchTx.Unlock()

	b.ResetTimer()
	defer b.StopTimer()
	for i := 0; i < b.N; i++ {
		batchTx.Lock()
		batchTx.UnsafePut(buckets.Test, []byte(generateValidKey(b)), newRandomBytes(256))
		batchTx.Unlock()
	}
}

var appliedIndex uint64

func Benchmark_RaftLSM_Backend_Put(b *testing.B) {
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
	defer func() {
		if err := be.Sync(); err != nil {
			b.Error(err)
		}
	}()
	b.ResetTimer()
	defer b.StopTimer()
	for i := 0; i < b.N; i++ {
		if err := be.Put(appliedIndex, generateValidKey(b), string(newRandomBytes(256))); err != nil {
			b.Error(err)
		}
		appliedIndex++
	}
}
