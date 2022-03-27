package ntfyutil

import (
	"github.com/vigilglc/raft-lsm/server/utils/syncutil"
	"sync"
)

type CyclicNotifier struct {
	rwmu sync.RWMutex
	ch   chan struct{}
}

func NewCyclicNotifier() *CyclicNotifier {
	return &CyclicNotifier{ch: make(chan struct{})}
}

func (ntf *CyclicNotifier) Wait() <-chan struct{} {
	defer syncutil.SchedLockers(ntf.rwmu.RLocker())()
	return ntf.ch
}

func (ntf *CyclicNotifier) Notify() {
	defer syncutil.SchedLockers(&ntf.rwmu)()
	close(ntf.ch)
	ntf.ch = make(chan struct{})
}
