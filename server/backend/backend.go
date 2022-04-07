package backend

import (
	"errors"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
	"github.com/vigilglc/raft-lsm/server/backend/codec"
	"github.com/vigilglc/raft-lsm/server/backend/kvpb"
	"github.com/vigilglc/raft-lsm/server/utils/syncutil"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"
	"io"
	"strings"
	"sync"
	"unicode"
)

type Backend interface {
	AppliedIndex() uint64
	CurrConfState() raftpb.ConfState
	PutConfState(ai uint64, confState raftpb.ConfState, meta ...*kvpb.KV) error

	Get(key string) (val string, err error)
	Put(ai uint64, key, val string) error
	Del(ai uint64, key string) error
	Write(batch *Batch) error
	// Range returns a channel conveying kv whose key ranges from 'from' to 'to'. Notice that 'from' is inclusive and
	// 'to' is exclusive. 'closeC' should be closed by the caller on aborting reading from 'kvC'.
	Range(from, to string, asc bool) (kvC <-chan *kvpb.KV, errC <-chan error, closeC chan<- struct{})

	SnapshotStream() (ai uint64, rc io.ReadCloser, size int64, err error)
	ReceiveSnapshot(ai uint64, rc io.ReadCloser) error

	Sync() error
	Close() error
}

type backend struct {
	lg *zap.Logger
	// basic db fields
	cfg    Config
	dbRwmu sync.RWMutex // useful when accepting snapshot. And it also protects appliedIndex and confState.
	db     *leveldb.DB
	// raft states as follows
	aiRwmu       sync.RWMutex
	appliedIndex uint64
	cfstRwmu     sync.RWMutex
	confState    raftpb.ConfState
	// closing...
	stopped chan struct{}
}

const (
	ReservedSPrefix = "__"
	appliedIndexKey = ReservedSPrefix + "APPLIED_INDEX"
	confStateKey    = ReservedSPrefix + "CONF_STATE"
	syncMarkKey     = ReservedSPrefix + "SYNC"
)

const (
	maxKeySize        int = 1000
	rangeChanBufSize  int = 64
	streamChanBufSize int = 128
)

var ErrInvalidKey = errors.New("backend: invalid key")
var ErrOutdatedWrite = errors.New("backend: out-of-date write")

func OpenBackend(lg *zap.Logger, cfg Config) (ret Backend, err error) {
	be := &backend{lg: lg, cfg: cfg, stopped: make(chan struct{})}
	if lg == nil {
		be.lg = zap.NewExample()
	}
	var mainDir string
	err = cfg.MakeDirAll()
	if err == nil {
		err = cfg.CleanDir()
	}
	if err == nil {
		mainDir, err = cfg.GetMainDir()
	}
	if err != nil {
		be.lg.Error("failed to get backend main dir", zap.Error(err))
		return
	}
	be.db, err = leveldb.OpenFile(mainDir, &opt.Options{
		NoSync:       false, // necessary, if global NoSync is true, all writes do no fSync.
		NoWriteMerge: true,  // channel write sequence is not guaranteed!
		Comparer:     cfg.GetComparer(),
	})
	if err != nil {
		be.lg.Error("failed to open leveldb", zap.Error(err))
		return
	}
	if be.appliedIndex, err = readInAppliedIndex(be.db); err != nil {
		be.lg.Error("failed to read appliedIndex in", zap.Error(err))
		return nil, err
	}
	confState, err := readInConfState(be.db)
	if err != nil {
		be.lg.Error("failed to read confState in", zap.Error(err))
		return nil, err
	}
	be.confState = *confState
	return be, nil
}

func ValidateKey(key string) bool {
	if len(key) > maxKeySize {
		return false
	}
	if strings.HasPrefix(key, ReservedSPrefix) {
		return false
	}
	for _, rn := range key {
		if unicode.IsSpace(rn) {
			return false
		}
	}
	return true
}

func (be *backend) AppliedIndex() uint64 {
	defer syncutil.SchedLockers(be.dbRwmu.RLocker(), be.aiRwmu.RLocker())()
	return be.appliedIndex
}
func readInAppliedIndex(db *leveldb.DB) (ai uint64, err error) {
	bts, err := db.Get([]byte(appliedIndexKey), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return 0, nil
		}
		return 0, err
	}
	return bytes2Uint64(bts), nil
}

func (be *backend) CurrConfState() raftpb.ConfState {
	defer syncutil.SchedLockers(be.dbRwmu.RLocker(), be.cfstRwmu.RLocker())()
	return be.confState
}
func readInConfState(db *leveldb.DB) (confState *raftpb.ConfState, err error) {
	confState = new(raftpb.ConfState)
	bts, err := db.Get([]byte(confStateKey), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return confState, nil
		}
		return confState, err
	}
	return bytes2ConfState(bts)
}

func (be *backend) PutConfState(ai uint64, confState raftpb.ConfState, meta ...*kvpb.KV) error {
	builder := NewBatchBuilder().AppliedIndex(ai).confState(&confState)
	for _, kv := range meta {
		builder.Put(kv.Key, kv.Val)
	}
	return be.Write(builder.Finish())
}

func (be *backend) Get(key string) (val string, err error) {
	defer syncutil.SchedLockers(be.dbRwmu.RLocker())()
	bts, err := be.db.Get([]byte(key), nil)
	return string(bts), err
}

func (be *backend) Put(ai uint64, key, val string) error {
	return be.Write(NewBatchBuilder().
		AppliedIndex(ai).Put(key, val).Finish(),
	)
}

func (be *backend) Del(ai uint64, key string) error {
	return be.Write(NewBatchBuilder().
		AppliedIndex(ai).Del(key).Finish(),
	)
}

func (be *backend) Write(batch *Batch) error {
	if len(batch.ents) == 0 {
		return nil
	}
	defer syncutil.SchedLockers(be.dbRwmu.RLocker())()
	be.aiRwmu.RLock()
	if be.appliedIndex >= batch.appliedIndex {
		defer be.aiRwmu.RUnlock()
		return nil
	}
	be.aiRwmu.RUnlock()
	internalBatch := new(leveldb.Batch)
	internalBatch.Put([]byte(appliedIndexKey), uint64ToBytes(batch.appliedIndex))
	for _, ent := range batch.ents {
		if ent.val == nil {
			internalBatch.Delete([]byte(ent.key))
		} else {
			internalBatch.Put([]byte(ent.key), []byte(*ent.val))
		}
	}
	if batch.confState != nil {
		bts, err := confState2Bytes(batch.confState)
		if err != nil {
			be.lg.Error("failed to marshal confState",
				zap.Any("confState", batch.confState),
				zap.Error(err),
			)
			return err
		}
		internalBatch.Put([]byte(confStateKey), bts)
	}
	defer syncutil.SchedLockers(&be.aiRwmu)()
	if be.appliedIndex >= batch.appliedIndex {
		return ErrOutdatedWrite
	}
	err := be.db.Write(internalBatch, &opt.WriteOptions{
		Sync:         be.cfg.GetSync(),
		NoWriteMerge: true,
	})
	if err != nil {
		be.lg.Error("failed to write leveldb batch",
			zap.Any("batch", internalBatch),
			zap.Error(err),
		)
		return err
	}
	be.appliedIndex = batch.appliedIndex
	if batch.confState != nil {
		defer syncutil.SchedLockers(&be.cfstRwmu)()
		be.confState = *batch.confState
	}
	return nil
}

var emptyRangeKVChan = make(chan *kvpb.KV)

func (be *backend) Range(from, to string, asc bool) (kvC <-chan *kvpb.KV, errC <-chan error, closeC chan<- struct{}) {
	if be.cfg.GetComparer().Compare([]byte(from), []byte(to)) >= 0 { // since there is no space between 'from' and 'to'
		return emptyRangeKVChan, make(chan error), make(chan struct{})
	}
	retKVC := make(chan *kvpb.KV, rangeChanBufSize)
	retErrC := make(chan error, 1)
	retCloseC := make(chan struct{})
	be.dbRwmu.RLock()
	iter := be.db.NewIterator(&util.Range{Start: []byte(from), Limit: []byte(to)}, nil)
	go func() {
		defer be.dbRwmu.RUnlock()
		defer iter.Release()
		pipeIterator2Chan(iter, asc, retKVC, retErrC, retCloseC, be.stopped)
	}()
	return retKVC, retErrC, retCloseC
}

func (be *backend) SnapshotStream() (ai uint64, rc io.ReadCloser, size int64, err error) {
	kvC, errC, closeC := make(chan *kvpb.KV, streamChanBufSize), make(chan error, 1), make(chan struct{})
	be.dbRwmu.RLock()
	defer syncutil.SchedLockers(be.aiRwmu.RLocker())()
	ai = be.appliedIndex
	szs, err := be.db.SizeOf([]util.Range{{}})
	if err != nil {
		be.lg.Warn("failed to calculate snapshot size of leveldb",
			zap.Error(err),
		)
		return ai, nil, 0, err
	}
	size = szs[0]
	iter := be.db.NewIterator(nil, nil)
	go func() {
		defer be.dbRwmu.RUnlock()
		defer iter.Release()
		pipeIterator2Chan(iter, true, kvC, errC, closeC, nil)
	}()
	pr, pw := io.Pipe()
	go func() {
		var err error
		enc := codec.NewEncoder(pw)
		for err == nil {
			select {
			case <-be.stopped:
				err = leveldb.ErrClosed
			case err = <-errC:
			case kv, ok := <-kvC:
				if !ok {
					goto FIN
				}
				err = enc.Encode(kv)
			}
		}
	FIN:
		close(closeC)
		if err == nil {
			if err = enc.Close(); err != nil {
				be.lg.Error("failed to close KV encoder", zap.Error(err))
			}
		}
		if err = pw.CloseWithError(err); err != nil {
			be.lg.Error("failed to close pipe writer", zap.Error(err))
			return
		}
	}()
	return ai, pr, size, err
}

func (be *backend) ReceiveSnapshot(ai uint64, rc io.ReadCloser) (err error) {
	if be.AppliedIndex() >= ai {
		return nil
	}
	tempDir, err := be.cfg.MakeTempDir(ai)
	if err != nil {
		be.lg.Error("failed to make temp dir", zap.String("tempDir", tempDir), zap.Error(err))
		return err
	}
	tempDB, err := leveldb.OpenFile(tempDir, &opt.Options{
		NoSync:       false,
		NoWriteMerge: true,
		Comparer:     be.cfg.GetComparer(),
	})
	if err != nil {
		be.lg.Error("failed to open temp leveldb", zap.Error(err))
		return err
	}
	var kv = new(kvpb.KV)
	dec := codec.NewDecoder(rc)
	for err == nil {
		err = dec.Decode(kv)
		if err != nil {
			break
		}
		err = tempDB.Put([]byte(kv.Key), []byte(kv.Val), nil)
	}
	if err == io.EOF {
		err = doSync(tempDB)
	}
	if err != nil {
		be.lg.Warn("failed to decode snapshot or build new tempDB", zap.Error(err))
		_ = tempDB.Close()
		return err
	}
	defer syncutil.SchedLockers(&be.dbRwmu, &be.aiRwmu, &be.cfstRwmu)()
	appliedIndex, err := readInAppliedIndex(be.db)
	if err != nil {
		be.lg.Error("failed to read appliedIndex in", zap.Error(err))
		_ = tempDB.Close()
		return err
	}
	if be.appliedIndex >= appliedIndex {
		return tempDB.Close()
	}
	confState, err := readInConfState(be.db)
	if err != nil {
		be.lg.Error("failed to read confState in", zap.Error(err))
		_ = tempDB.Close()
		return err
	}
	if err := be.cfg.SetMainDir(tempDir); err != nil {
		be.lg.Error("failed to read confState in", zap.Error(err))
		_ = tempDB.Close()
		return err
	}
	var oldDB = be.db
	be.db = tempDB
	be.appliedIndex, be.confState = appliedIndex, *confState
	return oldDB.Close()
}

func (be *backend) Sync() error {
	defer syncutil.SchedLockers(be.dbRwmu.RLocker())()
	return doSync(be.db)
}

func doSync(db *leveldb.DB) error {
	return db.Put([]byte(syncMarkKey), nil, &opt.WriteOptions{
		Sync:         true, // force fsync!
		NoWriteMerge: true,
	})
}

func (be *backend) Close() error {
	if be.cfg.GetForceClose() {
		close(be.stopped)
	}
	defer syncutil.SchedLockers(&be.dbRwmu)()
	return be.db.Close()
}

func init() {
	close(emptyRangeKVChan)
}
