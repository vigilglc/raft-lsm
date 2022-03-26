package raftn

import (
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/etcdserver"
	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
	"go.uber.org/zap"
	"sync"
	"time"
)

type RaftNode struct {
	lg         *zap.Logger
	n          raft.Node
	transport  *rafthttp.Transport
	memStorage *raft.MemoryStorage
	walStorage etcdserver.Storage
	// logical timer
	oneTick time.Duration
	tickMu  sync.Mutex
	ticker  *time.Ticker
	// stop chan
	stopped chan struct{}
	done    chan struct{}
	// functional channel...
	applyPatchC chan *ApplyPatch
	readStatesC chan []raft.ReadState // read index...
}

func NewRaftNode(
	lg *zap.Logger, oneTick time.Duration,
	node raft.Node, transport *rafthttp.Transport,
	memStorage *raft.MemoryStorage, walStorage etcdserver.Storage) *RaftNode {
	if lg == nil {
		lg = zap.NewExample()
	}
	r := &RaftNode{
		lg: lg, oneTick: oneTick,
		n: node, transport: transport,
		memStorage: memStorage, walStorage: walStorage,
	}
	raft.SetLogger(&ZapRaftLogger{lg.Sugar()})
	r.ticker = time.NewTicker(oneTick)
	return r
}

type DataBridge struct {
	setLead           func(lead uint64)
	setCommittedIndex func(cidx uint64)
}

type ApplyPatch struct {
	CommittedEntries []raftpb.Entry
	Snapshot         raftpb.Snapshot
	SnapMsgs         []raftpb.Message
	SnapSyncedC      <-chan struct{} // raft notify server nonempty snapshot is persisted...
	EntsAppliedC     chan<- struct{} // server notify raft entries already applied...
}

func (r *RaftNode) Start(bridge DataBridge) {
	var onStop = func() {
		r.n.Stop()
		r.ticker.Stop()
		r.transport.Stop()
		if err := r.walStorage.Close(); err != nil {
			r.lg.Panic("failed to close Raft storage", zap.Error(err))
		}
		close(r.done)
	}
	go func() {
		defer onStop()
		var lead uint64 = 0
		var isLead bool

		for true {
			select {
			case <-r.ticker.C:
				r.tick()
			case rd := <-r.n.Ready():
				if rd.SoftState != nil {
					if rd.SoftState.Lead != raft.None && rd.SoftState.Lead != lead {
						lead = rd.SoftState.Lead
						bridge.setLead(lead)
					}
					isLead = rd.SoftState.RaftState == raft.StateLeader
				}
				if len(rd.ReadStates) != 0 {
					select {
					case r.readStatesC <- rd.ReadStates:
					case <-r.stopped:
						return
					}
				}
				nonSnaps, snapMsgs := r.processMessages(r.transport.Raft.IsIDRemoved, rd.Messages)
				snapSyncedC, entsAppliedC := make(chan struct{}), make(chan struct{})
				patch := ApplyPatch{
					CommittedEntries: rd.CommittedEntries,
					Snapshot:         rd.Snapshot,
					SnapMsgs:         snapMsgs,
					SnapSyncedC:      snapSyncedC,
					EntsAppliedC:     entsAppliedC,
				}
				select {
				case r.applyPatchC <- &patch:
				case <-r.stopped:
					return
				}
				if isLead {
					r.transport.Send(nonSnaps)
				}
				if !raft.IsEmptySnap(rd.Snapshot) {
					if err := r.walStorage.SaveSnap(rd.Snapshot); err != nil {
						r.lg.Fatal("failed to save Raft snapshot", zap.Error(err))
					}
				}
				if err := r.walStorage.Save(rd.HardState, rd.Entries); err != nil { // invokes sync only voted or term changed...
					r.lg.Fatal("failed to save Raft hardState and entries", zap.Error(err))
				}
				if !raft.IsEmptySnap(rd.Snapshot) {
					if err := r.walStorage.Sync(); err != nil {
						r.lg.Fatal("failed to sync Raft snapshot", zap.Error(err))
					}
					close(snapSyncedC)
					_ = r.memStorage.ApplySnapshot(rd.Snapshot)
					if err := r.walStorage.Release(rd.Snapshot); err != nil {
						r.lg.Fatal("failed to release Raft wal", zap.Error(err))
					}
				}
				_ = r.memStorage.Append(rd.Entries)
				if !isLead {
					if mustWaitApply(rd.CommittedEntries) {
						select {
						case <-entsAppliedC:
						case <-r.stopped:
							return
						}
					}
					r.transport.Send(nonSnaps)
				}
				r.n.Advance()
			case <-r.stopped:
				return
			}
		}
	}()
}

func (r *RaftNode) ApplyPatchC() <-chan *ApplyPatch {
	return r.applyPatchC
}

func (r *RaftNode) ReadStatesC() <-chan []raft.ReadState {
	return r.readStatesC
}

func (r *RaftNode) Stop() {
	close(r.stopped)
	<-r.done
}

func (r *RaftNode) Status() raft.Status {
	return r.n.Status()
}

func (r *RaftNode) processMessages(isIDRemoved func(id uint64) bool, msgs []raftpb.Message) (
	nonSnaps []raftpb.Message, snapMsgs []raftpb.Message) {
	appRespM := map[uint64]int{}
	for i := 0; i < len(msgs); i++ {
		if isIDRemoved(msgs[i].To) {
			msgs[i].To = 0
			continue
		}
		msgTp := msgs[i].Type
		if msgTp == raftpb.MsgSnap {
			snapMsgs = append(snapMsgs, msgs[i])
			msgs[i].To = 0
		} else if msgTp == raftpb.MsgAppResp {
			if idx, ok := appRespM[msgs[i].To]; ok {
				msgs[idx].To = 0
			}
			appRespM[msgs[i].To] = i
		}
	}
	return msgs, snapMsgs
}

func mustWaitApply(committedEnts []raftpb.Entry) bool {
	for _, ent := range committedEnts {
		if ent.Type == raftpb.EntryConfChange || ent.Type == raftpb.EntryConfChangeV2 {
			return true
		}
	}
	return false
}

func (r *RaftNode) tick() {
	r.tickMu.Lock()
	defer r.tickMu.Unlock()
	r.n.Tick()
}
