package ntfyutil

import (
	"sync"
	"testing"
)

func TestTimelineNotifier(t *testing.T) {
	var wg sync.WaitGroup
	notifier := new(TimelineNotifier)
	for _, time := range [...]uint64{1, 2, 3, 4, 5, 6, 7, 8, 9} {
		go func(time uint64, ch <-chan struct{}) {
			<-ch
			t.Logf("time chan: %d released\n", time)
			wg.Done()
		}(time, notifier.Wait(time))
	}
	wg.Add(4)
	notifier.Notify(4)
	wg.Wait()
	wg.Add(4)
	notifier.Notify(8)
	wg.Wait()
	<-notifier.Wait(8)
	wg.Add(1)
	notifier.Notify(10)
	wg.Wait()
}
