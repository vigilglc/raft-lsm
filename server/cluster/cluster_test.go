package cluster

import (
	"github.com/vigilglc/raft-lsm/server/backend"
	"go.uber.org/zap"
	"os"
	"path/filepath"
	"testing"
)

var testingClusterBackendDir string

func TestMain(m *testing.M) {
	gopath, set := os.LookupEnv("GOPATH")
	if !set {
		os.Exit(-1)
	}
	testingClusterBackendDir = filepath.Join(gopath, "temp", "testing", "cluster")
	_ = os.RemoveAll(testingClusterBackendDir)
	if err := os.MkdirAll(testingClusterBackendDir, 666); err != nil {
		os.Exit(-1)
	}
	m.Run()
	_ = os.RemoveAll(testingClusterBackendDir)
}

func TestBuildCluster(t *testing.T) {
	lg := zap.NewExample()
	be, err := backend.OpenBackend(lg, backend.Config{
		Dir: testingClusterBackendDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(testingClusterBackendDir) }()
	defer func(be backend.Backend) {
		if err := be.Close(); err != nil {
			t.Fatal(err)
		}
	}(be)
	clusterName := "raft-lsm"
	localMem := NewMember(clusterName, AddrInfo{
		Name:        "node-0",
		Host:        "localhost",
		ServicePort: 400,
		RaftPort:    8000,
	}, false)
	peerMems := []*Member{
		NewMember(clusterName, AddrInfo{
			Name:        "node-1",
			Host:        "localhost",
			ServicePort: 401,
			RaftPort:    8001,
		}, false),
		NewMember(clusterName, AddrInfo{
			Name:        "node-2",
			Host:        "localhost",
			ServicePort: 402,
			RaftPort:    8002,
		}, false),
	}
	builder := NewClusterBuilder(lg, clusterName, be)
	builder.AddMember(localMem.AddrInfo)
	for _, mem := range peerMems {
		builder.AddMember(mem.AddrInfo)
	}
	builder.SetLocalMember(localMem.ID)
	cluster, err := builder.Finish()
	if err != nil {
		t.Fatal(err)
	}
	for _, mem := range cluster.GetMembers() {
		t.Logf("%v\n", mem)
	}
}
