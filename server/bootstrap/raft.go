package bootstrap

import (
	json "github.com/json-iterator/go"
	"github.com/vigilglc/raft-lsm/server/cluster"
	"github.com/vigilglc/raft-lsm/server/config"
	"go.etcd.io/etcd/raft/v3"
	"go.uber.org/zap"
)

type BootstrappedRaft struct {
	startWithPeers bool
	raftConfig     *raft.Config
	peers          []raft.Peer
}

const (
	// The max throughput of etcd will not exceed 100MB/s (100K * 1KB value).
	// Assuming the RTT is around 10ms, 1MB max size is large enough.
	maxSizePerMsg = 1 * 1024 * 1024
	// Never overflow the rafthttp buffer, which is 4096.
	// TODO: a better const?
	maxInflightMsgs = 4096 / 8
)

func bootstrapRaft(cfg *config.ServerConfig, haveWAL bool, cl *cluster.Cluster,
	memStorage *raft.MemoryStorage) (*BootstrappedRaft, error) {
	lg := cfg.GetLogger()
	btRaft := new(BootstrappedRaft)
	btRaft.startWithPeers = !haveWAL && cfg.NewCluster
	if btRaft.startWithPeers {
		for _, mem := range cl.GetMembers() {
			ctx, err := json.Marshal(mem)
			if err != nil {
				lg.Error("failed to marshal member", zap.Error(err))
				return nil, err
			}
			btRaft.peers = append(btRaft.peers, raft.Peer{ID: mem.ID, Context: ctx})
		}
	}
	btRaft.raftConfig = &raft.Config{
		ID:              cl.GetLocalMemberID(),
		ElectionTick:    cfg.ElectionTicks,
		HeartbeatTick:   cfg.HeartbeatTicks,
		Storage:         memStorage,
		MaxSizePerMsg:   maxSizePerMsg,
		MaxInflightMsgs: maxInflightMsgs,
		CheckQuorum:     true,
		PreVote:         true,
	}
	return btRaft, nil
}

func (btRaft *BootstrappedRaft) StartRaft() raft.Node {
	if btRaft.startWithPeers {
		return raft.StartNode(btRaft.raftConfig, btRaft.peers)
	}
	return raft.RestartNode(btRaft.raftConfig)
}
