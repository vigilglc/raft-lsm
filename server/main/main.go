package main

import (
	"fmt"
	"github.com/vigilglc/raft-lsm/server/api"
	"github.com/vigilglc/raft-lsm/server/config"
	"github.com/vigilglc/raft-lsm/server/main/cmd"
	"log"
)

func main() {
	var cfg *config.ServerConfig
	if err := cmd.Parse(func(cmdFlags *cmd.CommandFlags) (err error) {
		if fn, set := cmdFlags.ConfFilePath(); set {
			cfg, err = config.ReadServerConfig(fn)
		} else {
			return fmt.Errorf("config flag not set")
		}
		if err != nil {
			return err
		}
		if b, set := cmdFlags.NewCluster(); set {
			cfg.NewCluster = b
		}
		if s, set := cmdFlags.DataDir(); set {
			cfg.DataDir = s
		}
		if b, set := cmdFlags.BackendSync(); set {
			cfg.BackendSync = b
		}
		if b, set := cmdFlags.Development(); set {
			cfg.Development = b
		}
		if s, set := cmdFlags.LogLevel(); set {
			cfg.LogLevel = s
		}
		if ss, set := cmdFlags.LogOutputPaths(); set {
			cfg.LogOutputPaths = ss
		}
		return
	}); err != nil {
		log.Fatal(err)
	}
	if err := api.StartService(cfg); err != nil {
		log.Fatal(err)
	}
}
