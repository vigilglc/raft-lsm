package server

import (
	"context"
	"github.com/vigilglc/raft-lsm/server/backend"
	"github.com/vigilglc/raft-lsm/server/bootstrap"
	"github.com/vigilglc/raft-lsm/server/cluster"
	"github.com/vigilglc/raft-lsm/server/config"
	"github.com/vigilglc/raft-lsm/server/raftn"
	"github.com/vigilglc/raft-lsm/server/utils/ntfyutil"
	"github.com/vigilglc/raft-lsm/server/utils/syncutil"
	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/server/v3/etcdserver"
	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v2stats"
	"go.uber.org/zap"
	"strconv"
	"time"
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
	Config      *config.ServerConfig
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

func NewServer(cfg *config.ServerConfig) *Server {
	lg := cfg.GetLogger()
	btSrv := bootstrap.Bootstrap(cfg)

	cl := btSrv.BootstrappedCluster.Cluster
	uintID := cl.GetLocalMemberID()
	strID := strconv.FormatUint(uintID, 10)
	serverStats := v2stats.NewServerStats(cfg.LocalAddrInfo.Name, strID)
	leaderStats := v2stats.NewLeaderStats(lg, strID)

	srv := &Server{
		// stats
		serverStats: serverStats,
		leaderStats: leaderStats,
		// core
		lg:      lg,
		Config:  cfg,
		applier: NewBackendApplier(lg, btSrv.Backend),
		cluster: cl,
		// @raftNode:
		backend:     btSrv.Backend,
		memStorage:  btSrv.MemStorage,
		walStorage:  etcdserver.NewStorage(btSrv.WAL, btSrv.Snapshotter),
		snapshotter: btSrv.Snapshotter,
		// @transport:
		// channel...
		readyC:         make(chan struct{}),
		stopped:        make(chan struct{}),
		done:           make(chan struct{}),
		tptErrorC:      make(chan error, 1),
		readIndexWaitC: make(chan context.Context),
		// req and resp
		reqIDGen:    ntfyutil.NewIDGenerator(uintID, time.Now()),
		reqNotifier: new(ntfyutil.RegisterNotifier),
		timelineNtf: new(ntfyutil.TimelineNotifier),
		// util
		fw:                 new(syncutil.FuncWatcher),
		leaderChangeNtf:    ntfyutil.NewCyclicNotifier(),
		readIndexSharedNtf: ntfyutil.NewSharedVEmitter(),
	}
	localURLs, err := types.NewURLs([]string{cfg.LocalAddrInfo.RaftAddress()})
	if err != nil {
		lg.Panic("failed to parse raft URL",
			zap.String("raft-address", cfg.LocalAddrInfo.RaftAddress()), zap.Error(err),
		)
	}
	srv.transport = &rafthttp.Transport{
		Logger:      lg,
		DialTimeout: cfg.GetPeerDialTimeout(),
		ID:          types.ID(uintID),
		URLs:        localURLs,
		ClusterID:   types.ID(cl.GetClusterID()),
		Raft:        nil, // TODO: implement Raft interface
		Snapshotter: srv.snapshotter,
		ServerStats: srv.serverStats,
		LeaderStats: srv.leaderStats,
		ErrorC:      srv.tptErrorC,
	}
	srv.raftNode = raftn.NewRaftNode(lg,
		cfg.GetOneTickMillis(),
		btSrv.BootstrappedRaft.StartRaft(), srv.transport,
		srv.memStorage, srv.walStorage,
	)
	return srv
}
