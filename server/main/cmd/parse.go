package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	"strings"
)

const (
	confFilePathFlag   = "config"
	newClusterFlag     = "new"
	dataDirFlag        = "dir"
	backendSyncFlag    = "sync"
	developmentFlag    = "dev"
	logLevelFlag       = "level"
	logOutputPathsFlag = "logout"
)

type CommandFlags struct {
	bFlags  map[string]bool
	sFlags  map[string]string
	ssFlags map[string][]string
}

func NewCommandFlags() *CommandFlags {
	return &CommandFlags{
		bFlags:  map[string]bool{},
		sFlags:  map[string]string{},
		ssFlags: map[string][]string{},
	}
}

func (cf *CommandFlags) ConfFilePath() (res string, set bool) {
	res, set = cf.sFlags[confFilePathFlag]
	return
}

func (cf *CommandFlags) NewCluster() (res bool, set bool) {
	res, set = cf.bFlags[confFilePathFlag]
	return
}

func (cf *CommandFlags) DataDir() (res string, set bool) {
	res, set = cf.sFlags[dataDirFlag]
	return
}

func (cf *CommandFlags) BackendSync() (res bool, set bool) {
	res, set = cf.bFlags[backendSyncFlag]
	return
}

func (cf *CommandFlags) Development() (res bool, set bool) {
	res, set = cf.bFlags[developmentFlag]
	return
}

func (cf *CommandFlags) LogLevel() (res string, set bool) {
	res, set = cf.sFlags[logLevelFlag]
	return
}

func (cf *CommandFlags) LogOutputPaths() (res []string, set bool) {
	res, set = cf.ssFlags[logOutputPathsFlag]
	return
}

type Hook func(cmdFlags *CommandFlags) error

var globalErr error

func setGlobalErr(err error) {
	if globalErr == nil {
		globalErr = err
	}
}

var localHook Hook
var (
	root = cobra.Command{
		Use: "raft-lsm-server --config file " +
			"[--new true|false] [--sync true|false] [--dev true|false] " +
			"[--dir path] [--level lv] [--logout out...]",
		Args: cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			var cmdFlags = NewCommandFlags()
			for _, sf := range [...]string{confFilePathFlag, dataDirFlag, logLevelFlag} {
				if val, err := cmd.Flags().GetString(sf); err == nil && len(val) != 0 {
					if val = strings.TrimSpace(val); len(val) == 0 {
						continue
					}
					cmdFlags.sFlags[sf] = strings.TrimSpace(val)
				}
			}
			for _, bf := range [...]string{newClusterFlag, backendSyncFlag, developmentFlag} {
				if sBool, err := cmd.Flags().GetString(bf); err == nil && len(sBool) != 0 {
					sBool = strings.ToLower(strings.TrimSpace(sBool))
					var val = false
					if sBool == "true" {
						val = true
					} else if sBool != "false" {
						setGlobalErr(fmt.Errorf("cmd flag %s, true or false expected", bf))
						return
					}
					cmdFlags.bFlags[bf] = val
				}
			}
			for _, ssf := range [...]string{logOutputPathsFlag} {
				if val, err := cmd.Flags().GetStringSlice(ssf); err == nil && len(val) != 0 {
					cmdFlags.ssFlags[ssf] = val
				}
			}
			setGlobalErr(localHook(cmdFlags))
		},
	}
)

func init() {
	root.Flags().String(confFilePathFlag, "", "server config file path")
	if err := root.MarkFlagRequired(confFilePathFlag); err != nil {
		panic("failed to mark flags required")
	}
	root.Flags().String(newClusterFlag, "", "join a new? cluster")
	root.Flags().String(backendSyncFlag, "", "whether to do backend fsync")
	root.Flags().String(developmentFlag, "", "development mode")
	root.Flags().String(dataDirFlag, "", "server data directory")
	root.Flags().String(logLevelFlag, "", "log level")
	root.Flags().StringSlice(logOutputPathsFlag, nil, "log output paths")

	root.SetUsageFunc(func(cmd *cobra.Command) error {
		var err error
		_, err = fmt.Fprintln(cmd.OutOrStderr(), `Usage:
  raft-lsm-server --config file [--new true|false] [--sync true|false] [--dev true|false] [--dir path] [--level lv] [--logout out...]`)
		_, err = fmt.Fprintln(cmd.OutOrStderr(), `Flags:
      --config    server config file path
      --new       join a new? cluster
      --sync   	  whether to do backend fsync
      --dev       development mode
  -h, --help      help for raft-lsm-server
      --dir       server data directory
      --level     log level
      --logout    log output paths`)
		return err
	})
}

func Parse(hook Hook) error {
	localHook = hook
	setGlobalErr(root.Execute())
	return globalErr
}
