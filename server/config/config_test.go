package config

import (
	"testing"
)

func TestDefaultServerConfig(t *testing.T) {
	defCfg := DefaultServerConfig()
	t.Logf("%+v", defCfg)
}
