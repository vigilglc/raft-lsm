package ntfyutil

import (
	"github.com/vigilglc/raft-lsm/server/utils/syncutil"
	"log"
	"sync"
)

const defaultNotifierSegments = 64

type RegisterNotifier struct {
	segNtfs [defaultNotifierSegments]segmentNotifier
}

func (ntf *RegisterNotifier) Register(ID uint64) <-chan interface{} {
	return ntf.segNtfs[ID%defaultNotifierSegments].register(ID)
}

func (ntf *RegisterNotifier) Notify(ID uint64, v interface{}) {
	ntf.segNtfs[ID%defaultNotifierSegments].notify(ID, v)
}

func (ntf *RegisterNotifier) IsRegistered(ID uint64) bool {
	return ntf.segNtfs[ID%defaultNotifierSegments].isRegistered(ID)
}

type segmentNotifier struct {
	rwmu  sync.RWMutex
	idChM map[uint64]chan interface{}
}

func (ntf *segmentNotifier) register(ID uint64) <-chan interface{} {
	defer syncutil.SchedLockers(&ntf.rwmu)()
	if ntf.idChM == nil {
		ntf.idChM = map[uint64]chan interface{}{}
	}
	if _, ok := ntf.idChM[ID]; ok {
		log.Panicf("RegisterNotifier: duplicate register ID:%d", ID)
	}
	ret := make(chan interface{}, 1)
	ntf.idChM[ID] = ret
	return ret
}

func (ntf *segmentNotifier) notify(ID uint64, v interface{}) {
	defer syncutil.SchedLockers(ntf.rwmu.RLocker())()
	if ntf.idChM == nil {
		return
	}
	if ch, ok := ntf.idChM[ID]; ok {
		delete(ntf.idChM, ID)
		ch <- v
		close(ch)
	}
}

func (ntf *segmentNotifier) isRegistered(ID uint64) bool {
	defer syncutil.SchedLockers(ntf.rwmu.RLocker())()
	if ntf.idChM == nil {
		return false
	}
	_, ok := ntf.idChM[ID]
	return ok
}
