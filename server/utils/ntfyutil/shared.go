package ntfyutil

import (
	"github.com/vigilglc/raft-lsm/server/utils/syncutil"
	"sync"
)

type SharedV struct {
	rwmu sync.RWMutex
	v    interface{}
	ch   chan struct{}
}

func (sv *SharedV) Wait() <-chan struct{} {
	defer syncutil.SchedLockers(sv.rwmu.RLocker())()
	return sv.ch
}

func (sv *SharedV) Notify(v interface{}) {
	defer syncutil.SchedLockers(&sv.rwmu)()
	sv.v = v
	close(sv.ch)
}

func (sv *SharedV) Value() interface{} {
	defer syncutil.SchedLockers(sv.rwmu.RLocker())()
	return sv.v
}

type SharedVEmitter struct {
	rwmu    sync.RWMutex
	locker  sync.Locker
	sharing bool
	sharedV *SharedV
}

func NewSharedVEmitter() *SharedVEmitter {
	ntf := &SharedVEmitter{}
	ntf.clean()
	return ntf
}

func (ntf *SharedVEmitter) clean() {
	ntf.locker = &ntf.rwmu
	ntf.sharing = false
	ntf.sharedV = &SharedV{
		ch: make(chan struct{}),
	}
}

func (ntf *SharedVEmitter) CurrentShared(firstDo func()) *SharedV {
	defer syncutil.SchedLockers(ntf.locker)()
	if !ntf.sharing {
		ntf.sharing = true
		ntf.locker = ntf.rwmu.RLocker()
		if firstDo != nil {
			firstDo()
		}
	}
	return ntf.sharedV
}

func (ntf *SharedVEmitter) NextShared() (oldShared *SharedV) {
	defer syncutil.SchedLockers(&ntf.rwmu)()
	oldShared = ntf.sharedV
	ntf.clean()
	return
}
