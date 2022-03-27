package ntfyutil

import (
	"testing"
)

func TestRegisterNotifier(t *testing.T) {
	notifier := new(RegisterNotifier)
	var ID uint64 = 1023
	ch := notifier.Register(ID)
	notifier.Notify(ID, "HELL WORLD!")
	t.Log(<-ch)
}
