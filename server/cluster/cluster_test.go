package cluster

import (
	"github.com/vigilglc/raft-lsm/server/backend"
	"go.etcd.io/etcd/raft/v3/raftpb"
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

func TestClusterConfChange(t *testing.T) {
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
	cl, err := NewClusterBuilder(lg, clusterName, be).AddMembers(localMem.AddrInfo).
		SetLocalMember(localMem.ID).Finish()
	if err != nil {
		t.Fatal(err)
	}
	if err := cl.PushMembers2Backend(); err != nil {
		t.Fatal(err)
	}
	if beMems, err := cl.membersFromBackend(); err == nil {
		actual := beMems[localMem.ID]
		if len(beMems) != 1 || actual == nil || *localMem != *actual {
			t.Fatalf("only one member %v expected from backend, actual: %v", localMem, actual)
		}
	} else {
		t.Fatal(err)
	}
	mem2Add1 := NewMember(clusterName, AddrInfo{
		Name:        "node-1",
		Host:        "localhost",
		ServicePort: 401,
		RaftPort:    8001,
	}, false)
	cl.AddMember(be.AppliedIndex()+1, new(raftpb.ConfState), mem2Add1)
	mem2Add2 := NewMember(clusterName, AddrInfo{
		Name:        "node-2",
		Host:        "localhost",
		ServicePort: 402,
		RaftPort:    8002,
	}, true)
	cl.AddMember(be.AppliedIndex()+1, new(raftpb.ConfState), mem2Add2)
	if beMems, err := cl.membersFromBackend(); err == nil {
		if len(beMems) != len(cl.GetMembers()) {
			t.Fatalf("backend members %v not equal to memory members %v", beMems, cl.GetMembers())
		}
		for _, expected := range cl.GetMembers() {
			actual := beMems[expected.ID]
			if actual == nil || *expected != *actual {
				t.Fatalf("member %v expected from backend, actual: %v", expected, actual)
			}
		}
	} else {
		t.Fatal(err)
	}
	cl.PromoteMember(be.AppliedIndex()+1, new(raftpb.ConfState), mem2Add2.ID)
	if beMems, err := cl.membersFromBackend(); err == nil {
		if beMems[mem2Add2.ID].IsLearner {
			t.Fatalf("member %v should be protomted to notlearner...", mem2Add2)
		}
	} else {
		t.Fatal(err)
	}
	if beRemoved, err := cl.removedIDsFromBackend(); err == nil {
		if len(beRemoved) != 0 {
			t.Fatalf("there should be no nodes got removed")
		}
	} else {
		t.Fatal(err)
	}
	clMems := cl.GetMembers()
	for i, mem := range clMems {
		cl.RemoveMember(be.AppliedIndex()+1, new(raftpb.ConfState), mem.ID)
		if beMems, err := cl.membersFromBackend(); err == nil {
			if len(beMems) != len(cl.GetMembers()) {
				t.Fatalf("backend members %v not equal to memory members %v", beMems, cl.GetMembers())
			}
		}
		if beRemoved, err := cl.removedIDsFromBackend(); err == nil {
			if len(beRemoved) != i+1 {
				t.Fatalf("failed to remove %v", mem)
			}
			if _, ok := beRemoved[mem.ID]; !ok {
				t.Fatalf("failed to remove %v", mem)
			}
		}
	}
}
