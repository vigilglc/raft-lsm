package server

import (
	"context"
	"github.com/vigilglc/raft-lsm/server/backend"
	"github.com/vigilglc/raft-lsm/server/cluster"
	"github.com/vigilglc/raft-lsm/server/config"
	"github.com/vigilglc/raft-lsm/server/raftn"
	"github.com/vigilglc/raft-lsm/server/utils/ntfyutil"
	"github.com/vigilglc/raft-lsm/server/utils/syncutil"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/server/v3/etcdserver"
	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v2stats"
	"go.uber.org/zap"
)

type Server struct {
	// stats
	inflightSnapshots int64
	term              uint64
	committedIndex    uint64
	serverStats       *v2stats.ServerStats
	leaderStats       *v2stats.LeaderStats
	// core
	lg          *zap.Logger
	Config      config.ServerConfig
	applier     Applier
	cluster     *cluster.Cluster
	raftNode    *raftn.RaftNode
	backend     backend.Backend
	memStorage  *raft.MemoryStorage
	walStorage  etcdserver.Storage
	snapshotter *snap.Snapshotter
	transport   *rafthttp.Transport
	// channel...
	readyC         chan struct{} // after Server
	stopped        chan struct{}
	done           chan struct{}
	tptErrorC      chan error // transport ErrorC
	readIndexWaitC chan context.Context
	// req and resp
	reqIDGen    *ntfyutil.IDGenerator
	reqNotifier *ntfyutil.RegisterNotifier
	timelineNtf *ntfyutil.TimelineNotifier
	// util
	fw                 *syncutil.FuncWatcher
	leaderChangeNtf    *ntfyutil.CyclicNotifier
	readIndexSharedNtf *ntfyutil.SharedVEmitter
}
