package ntfyutil

import (
	"container/heap"
	"github.com/vigilglc/raft-lsm/server/utils/mathutil"
	"github.com/vigilglc/raft-lsm/server/utils/syncutil"
	"sync"
)

type TimelineNotifier struct {
	mu          sync.Mutex
	currentTime uint64
	tchH        timeChanHeap
}

var emptyStructChan = make(chan struct{})

func init() {
	close(emptyStructChan)
}
func (ntf *TimelineNotifier) Wait(time uint64) <-chan struct{} {
	defer syncutil.SchedLockers(&ntf.mu)()
	if time <= ntf.currentTime {
		return emptyStructChan
	}
	ret := make(chan struct{})
	heap.Push(&ntf.tchH, timeChan{time, ret})
	return ret
}

func (ntf *TimelineNotifier) Notify(time uint64) {
	defer syncutil.SchedLockers(&ntf.mu)()
	ntf.currentTime = mathutil.MaxUint64(time, ntf.currentTime)
	for !ntf.tchH.Empty() &&
		ntf.tchH.Peek().time <= ntf.currentTime {
		close(heap.Pop(&ntf.tchH).(timeChan).ch)
	}
}

type timeChan struct {
	time uint64
	ch   chan struct{}
}

type timeChanHeap []timeChan

func (tchH timeChanHeap) Empty() bool {
	return len(tchH) == 0
}

func (tchH timeChanHeap) Peek() timeChan {
	return tchH[0]
}

// region timeChanHeap Interface impls...

func (tchH *timeChanHeap) Len() int {
	return len(*tchH)
}

func (tchH *timeChanHeap) Less(i, j int) bool {
	if (*tchH)[i].time < (*tchH)[j].time {
		return true
	}
	return false
}

func (tchH *timeChanHeap) Swap(i, j int) {
	(*tchH)[i], (*tchH)[j] = (*tchH)[j], (*tchH)[i]
}

func (tchH *timeChanHeap) Push(x interface{}) {
	*tchH = append(*tchH, x.(timeChan))
}

func (tchH *timeChanHeap) Pop() interface{} {
	size := len(*tchH)
	ret := (*tchH)[size-1]
	*tchH = (*tchH)[:size-1]
	return ret
}

// endregion
