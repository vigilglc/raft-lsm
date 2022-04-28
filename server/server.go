package server

import (
	"context"
	"fmt"
	"github.com/vigilglc/raft-lsm/server/backend"
	"github.com/vigilglc/raft-lsm/server/bootstrap"
	"github.com/vigilglc/raft-lsm/server/cluster"
	"github.com/vigilglc/raft-lsm/server/config"
	"github.com/vigilglc/raft-lsm/server/raftn"
	scheduler "github.com/vigilglc/raft-lsm/server/scheduler"
	"github.com/vigilglc/raft-lsm/server/utils/ntfyutil"
	"github.com/vigilglc/raft-lsm/server/utils/syncutil"
	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/etcdserver"
	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v2http/httptypes"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v2stats"
	"go.uber.org/zap"
	"net"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"
)

type Server struct {
	// stats
	inflightSnapshots int64
	lead              uint64
	term              uint64
	committedIndex    uint64
	serverStats       *v2stats.ServerStats
	leaderStats       *v2stats.LeaderStats
	// core
	lg           *zap.Logger
	Config       *config.ServerConfig
	applier      Applier
	appliedIndex uint64
	cluster      *cluster.Cluster
	raftNode     *raftn.RaftNode
	backend      backend.Backend
	memStorage   *raft.MemoryStorage
	walStorage   etcdserver.Storage
	snapshotter  *snap.Snapshotter
	transport    *rafthttp.Transport
	// channel...
	readyC         chan struct{} // after Server
	stopped        chan struct{}
	done           chan struct{}
	errorC         chan error // transport ErrorC
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
	btSrv, err := bootstrap.Bootstrap(cfg)
	if err != nil {
		_ = btSrv.Close()
		lg.Panic("failed to bootstrap server", zap.Error(err))
	}
	cl := btSrv.BootstrappedCluster.Cluster
	uintID := cl.GetLocalMemberID()
	strID := strconv.FormatUint(uintID, 10)
	serverStats := v2stats.NewServerStats(cfg.LocalAddrInfo.Name, strID)
	leaderStats := v2stats.NewLeaderStats(lg, strID)

	var appliedIndex, _ = btSrv.MemStorage.FirstIndex()
	appliedIndex--
	srv := &Server{
		// stats
		serverStats: serverStats,
		leaderStats: leaderStats,
		// core
		lg:           lg,
		Config:       cfg,
		applier:      NewBackendApplier(lg, btSrv.Backend),
		appliedIndex: appliedIndex,
		cluster:      cl,
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
		errorC:         make(chan error, 1),
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
	localURLs, err := types.NewURLs([]string{cfg.LocalAddrInfo.HttpRaftAddress()})
	if err != nil {
		lg.Panic("failed to parse raft URL",
			zap.String("raft-address", cfg.LocalAddrInfo.HttpRaftAddress()), zap.Error(err),
		)
	}
	srv.transport = &rafthttp.Transport{
		Logger:      lg,
		DialTimeout: cfg.GetPeerDialTimeout(),
		ID:          types.ID(uintID),
		URLs:        localURLs,
		ClusterID:   types.ID(cl.GetClusterID()),
		Raft:        srv,
		Snapshotter: srv.snapshotter,
		ServerStats: srv.serverStats,
		LeaderStats: srv.leaderStats,
		ErrorC:      srv.errorC,
	}
	if err := srv.transport.Start(); err != nil {
		lg.Panic("failed to start transport", zap.Error(err))
	}
	for _, rmt := range btSrv.BootstrappedCluster.Remotes {
		if rmt.ID != uintID && rmt.ID != raft.None {
			srv.transport.AddRemote(types.ID(rmt.ID), []string{rmt.HttpRaftAddress()})
		}
	}
	for _, mem := range cl.GetMembers() {
		if mem.ID != uintID && mem.ID != raft.None {
			srv.transport.AddPeer(types.ID(mem.ID), []string{mem.HttpRaftAddress()})
		}
	}
	srv.raftNode = raftn.NewRaftNode(lg,
		cfg.GetOneTickMillis(),
		btSrv.BootstrappedRaft.StartRaft(), srv.transport,
		srv.memStorage, srv.walStorage,
	)
	return srv
}

func (s *Server) Start() {
	s.fw.Attach(s.linearizableReadIndexLoop)
	s.fw.Attach(s.run)
}

func (s *Server) Stop() {
	close(s.stopped)
	s.fw.Wait()
	<-s.done
}

func (s *Server) run() {
	sched := scheduler.NewParallelScheduler(context.Background(), 2)

	defer func() {
		sched.Stop()
		s.raftNode.Stop()
		err := s.backend.Close()
		if err != nil {
			s.lg.Error("failed to close backend", zap.Error(err))
		}
		if err := s.walStorage.Close(); err != nil {
			s.lg.Error("failed to close wal storage", zap.Error(err))
		}
	}()
	s.raftNode.Start(raftn.DataBridge{
		SetLead: func(lead uint64) {
			oldLead := atomic.LoadUint64(&s.lead)
			if oldLead != lead {
				atomic.StoreUint64(&s.lead, lead)
				s.leaderChangeNtf.Notify()
			}
		},
		SetCommittedIndex: func(cidx uint64) {
			if atomic.LoadUint64(&s.committedIndex) < cidx {
				atomic.StoreUint64(&s.committedIndex, cidx)
			}
		},
	})
	go func() { s.serverRaft() }()
	for true {
		select {
		case ap := <-s.raftNode.ApplyPatchC():
			sched.Schedule(scheduler.Job{Meta: 0, Func: func(ctx context.Context) {
				s.applyAll(ap)
			}})
		case rs := <-s.raftNode.ReadStatesC():
			sched.Schedule(scheduler.Job{Meta: 1, Func: func(ctx context.Context) {
				s.processReadStates(rs)
			}})
		case err := <-s.errorC:
			s.lg.Warn("server error", zap.Error(err))
			return
		case <-s.stopped:
			return
		}
	}
}

func (s *Server) serverRaft() {
	localMem := s.cluster.GetLocalMember()
	addr := fmt.Sprintf("%v:%v", localMem.Host, localMem.RaftPort)
	ls, err := net.Listen("tcp", addr)
	if err != nil {
		s.errorC <- err
	}
	err = (&http.Server{Handler: s.transport.Handler()}).Serve(ls)
	s.errorC <- err
}

// region rafthttp.Raft implementation

func (s *Server) Process(ctx context.Context, m raftpb.Message) error {
	if s.cluster.IsIDRemoved(m.From) {
		s.lg.Warn("cannot process message from removed member")
		return httptypes.NewHTTPError(http.StatusForbidden, "cannot process message from removed member")
	}
	return s.raftNode.Step(ctx, m)
}

func (s *Server) IsIDRemoved(id uint64) bool {
	return s.cluster.IsIDRemoved(id)
}

func (s *Server) ReportUnreachable(id uint64) {
	s.raftNode.ReportUnreachable(id)
}

func (s *Server) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	s.raftNode.ReportSnapshot(id, status)
}

// endregion
