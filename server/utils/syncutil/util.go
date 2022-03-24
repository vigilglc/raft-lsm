package syncutil

import "sync"

func SchedLockers(lcks ...sync.Locker) (unlock func()) {
	for i := 0; i < len(lcks); i++ {
		lcks[i].Lock()
	}
	return func() {
		for i := len(lcks) - 1; i >= 0; i-- {
			lcks[i].Unlock()
		}
	}
}

type FuncWatcher struct {
	sync.WaitGroup
}

func (fw *FuncWatcher) Attach(f func()) {
	fw.Add(1)
	go func() {
		defer fw.Done()
		f()
	}()
}
func (fw *FuncWatcher) RLocker() sync.Locker {
	return (*funcWatcherRLocker)(fw)
}

type funcWatcherRLocker FuncWatcher

func (fwa *funcWatcherRLocker) Lock() {
	fwa.Add(1)
}

func (fwa *funcWatcherRLocker) Unlock() {
	fwa.Done()
}
