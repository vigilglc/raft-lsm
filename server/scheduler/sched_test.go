package scheduler

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
)

func TestFifo(t *testing.T) {
	var JOBS = 1000
	var expected, actual uint64
	for i := 0; i < JOBS; i++ {
		expected += uint64(i)
	}

	var rwmu = sync.RWMutex{}
	var cond = sync.NewCond(rwmu.RLocker())
	var scheduled, finished uint64
	f := newFifo(context.Background(), &scheduled, &finished, &rwmu, cond)
	for i := 0; i < JOBS; i++ {
		local := i
		f.schedule(Job{Func: func(ctx context.Context) {
			atomic.AddUint64(&actual, uint64(local))
		}})
	}
	f.waitFinish(JOBS)
	f.stop()
	if actual != expected {
		t.Fatalf("expected: %v, actual: %v", expected, actual)
	}
}

func TestParallel(t *testing.T) {
	var JOBS = 1000
	var WAYS = 2
	var expected, actual uint64
	for i := 0; i < JOBS; i++ {
		expected += uint64(i)
	}

	sched := NewParallelScheduler(context.Background(), uint8(WAYS))
	for i := 0; i < JOBS; i++ {
		local := i
		sched.Schedule(Job{Meta: local, Func: func(ctx context.Context) {
			atomic.AddUint64(&actual, uint64(local))
		}})
	}
	sched.WaitFinish(JOBS)
	sched.Stop()
	if actual != expected {
		t.Fatalf("expected: %v, actual: %v", expected, actual)
	}
}
