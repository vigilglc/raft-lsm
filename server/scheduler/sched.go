package scheduler

import (
	"context"
	"github.com/vigilglc/raft-lsm/server/utils/syncutil"
	"sync"
	"sync/atomic"
)

type Job struct {
	Meta interface{}
	Func func(ctx context.Context)
}
type Scheduler interface {
	// Schedule asks the scheduler to schedule a job defined by the given func.
	// Schedule to a stopped scheduler might panic.
	Schedule(job Job)

	// Scheduled returns the number of scheduled jobs.
	Scheduled() int

	// Finished returns the number of finished jobs.
	Finished() int

	// WaitFinish waits until at least n job are finished.
	WaitFinish(n int)

	// Stop stops the scheduler.
	Stop()
}

type fifo struct {
	ctx     context.Context
	qMu     sync.Mutex
	qCond   *sync.Cond
	queue   []Job
	stopped uint64 // 0 for running, 1 for stopped...
	done    chan struct{}

	scheduled *uint64
	fWLocker  sync.Locker
	fRCond    *sync.Cond
	finished  *uint64
}

func newFifo(ctx context.Context, scheduled, finished *uint64, fWLocker sync.Locker, fRCond *sync.Cond) *fifo {
	ret := &fifo{
		ctx:       ctx,
		done:      make(chan struct{}),
		scheduled: scheduled,
		finished:  finished,
		fWLocker:  fWLocker,
		fRCond:    fRCond,
	}
	ret.qCond = sync.NewCond(&ret.qMu)
	go ret.run()
	return ret
}

func (f *fifo) schedule(job Job) {
	defer syncutil.SchedLockers(&f.qMu)()
	f.queue = append(f.queue, job)
	atomic.AddUint64(f.scheduled, 1)
	f.qCond.Broadcast()
}

func (f *fifo) waitFinish(n int) {
	defer syncutil.SchedLockers(f.fRCond.L)()
	for int(*(f.finished)) < n {
		f.fRCond.Wait()
	}
}

func (f *fifo) stop() {
	atomic.AddUint64(&f.stopped, 1)
	f.qCond.Broadcast() // not necessary to hold the lock!
	<-f.done
}

func (f *fifo) run() {
	defer close(f.done)
	for atomic.LoadUint64(&f.stopped) == 0 {
		f.qMu.Lock()
		for len(f.queue) == 0 && atomic.LoadUint64(&f.stopped) == 0 {
			f.qCond.Wait()
		}
		q := f.queue
		f.qMu.Unlock()
		var idx int
		for idx = 0; idx < len(q) && atomic.LoadUint64(&f.stopped) == 0; {
			q[idx].Func(f.ctx)
			f.fWLocker.Lock()
			*f.finished++
			f.fRCond.Broadcast()
			f.fWLocker.Unlock()
			idx++
		}
		f.qMu.Lock()
		f.queue = f.queue[idx:]
		f.qMu.Unlock()
	}
}

type parallel struct {
	ctx       context.Context
	cancelF   context.CancelFunc
	scheduled uint64
	finished  uint64
	fRCond    *sync.Cond
	subScheds []*fifo
}

func NewParallelScheduler(ctx context.Context, ways uint8) Scheduler {
	ret := new(parallel)
	ret.ctx, ret.cancelF = context.WithCancel(ctx)
	rwmu := sync.RWMutex{}
	ret.fRCond = sync.NewCond(rwmu.RLocker())
	ret.subScheds = make([]*fifo, ways)
	for i := 0; i < int(ways); i++ {
		ret.subScheds[i] = newFifo(ret.ctx, &ret.scheduled, &ret.finished, &rwmu, ret.fRCond)
	}
	return ret
}
func (p *parallel) Schedule(job Job) {
	p.subScheds[job.Meta.(int)%len(p.subScheds)].schedule(job)
}

func (p *parallel) Scheduled() int {
	return int(atomic.LoadUint64(&p.scheduled))
}

func (p *parallel) Finished() int {
	defer syncutil.SchedLockers(p.fRCond.L)()
	return int(p.finished)
}

func (p *parallel) WaitFinish(n int) {
	defer syncutil.SchedLockers(p.fRCond.L)()
	for int(p.finished) < n {
		p.fRCond.Wait()
	}
}

func (p *parallel) Stop() {
	if p.cancelF != nil {
		p.cancelF()
		p.cancelF = nil
	}
	for _, sub := range p.subScheds {
		sub.stop()
	}
}
