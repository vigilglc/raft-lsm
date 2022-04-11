package bootstrap

import (
	"github.com/vigilglc/raft-lsm/server/api/rpcpb"
	"github.com/vigilglc/raft-lsm/server/cluster"
	"github.com/vigilglc/raft-lsm/server/config"
	"log"
	"os"
	"path/filepath"
	"testing"
)

var testConfig *config.ServerConfig

func TestMain(m *testing.M) {
	var err error
	testConfig, err = config.ReadServerConfig("./test.toml")
	if err != nil {
		log.Fatalln(err)
	}
	gopath, set := os.LookupEnv("GOPATH")
	if !set {
		os.Exit(-1)
	}
	testingDataDir := filepath.Join(gopath, "temp", "testing", "bootstrap-new-cluster")
	testConfig.DataDir = testingDataDir
	if err := config.Validate(testConfig); err != nil {
		log.Fatalln(err)
	}
	testConfig.MakeLogger()
	m.Run()
	_ = os.RemoveAll(testingDataDir)
}

func TestBootstrapNewClusterNewNode(t *testing.T) {
	var err error
	oldNewCluster := testConfig.NewCluster
	testConfig.NewCluster = true
	defer func() {
		testConfig.NewCluster = oldNewCluster
	}()
	btSrv, err := Bootstrap(testConfig)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if err := btSrv.Close(); err != nil {
		t.Fatalf("%v", err)
	}
}

func TestBootstrapOldClusterNewNode(t *testing.T) {
	var err error
	oldNewCluster := testConfig.NewCluster
	oldClusterStatusFetcher := defaultClusterStatusFetcher
	defer func() {
		testConfig.NewCluster = oldNewCluster
		defaultClusterStatusFetcher = oldClusterStatusFetcher
	}()
	testConfig.NewCluster = false
	defaultClusterStatusFetcher = func(cfg *config.ServerConfig, mem *cluster.Member) (status *rpcpb.ClusterStatusResponse, err error) {
		return &rpcpb.ClusterStatusResponse{
			ID:   1023,
			Name: "raft-lsm-1023",
			Members: []*rpcpb.Member{
				{ID: 1, Name: "test-node-1", Host: "localhost", RaftPort: 401, ServicePort: 8001},
				{ID: 2, Name: "test-node-2", Host: "localhost", RaftPort: 402, ServicePort: 8002},
			},
			RemovedIDs: []uint64{1},
		}, nil
	}
	btSrv, err := Bootstrap(testConfig)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if err := btSrv.Close(); err != nil {
		t.Fatalf("%v", err)
	}
}
