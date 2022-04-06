package cmd

import (
	"github.com/spf13/cobra"
	"os"
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
var localHook Hook
var (
	root = cobra.Command{
		Use:  os.Args[0],
		Args: cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			var cmdFlags = new(CommandFlags)
			for _, sf := range [...]string{confFilePathFlag, dataDirFlag, logLevelFlag} {
				if val, err := cmd.Flags().GetString(sf); err == nil {
					cmdFlags.sFlags[sf] = val
				}
			}
			for _, bf := range [...]string{newClusterFlag, backendSyncFlag, developmentFlag} {
				if val, err := cmd.Flags().GetBool(bf); err == nil {
					cmdFlags.bFlags[bf] = val
				}
			}
			for _, ssf := range [...]string{logOutputPathsFlag} {
				if val, err := cmd.Flags().GetStringSlice(ssf); err == nil {
					cmdFlags.ssFlags[ssf] = val
				}
			}
			globalErr = localHook(cmdFlags)
		},
	}
)

func init() {
	root.Flags().String(confFilePathFlag, "", "server config file path")
	_ = root.MarkFlagRequired(confFilePathFlag)
	root.Flags().Bool(newClusterFlag, true, "join a new? cluster")
	root.Flags().String(dataDirFlag, "", "server data directory")
	root.Flags().Bool(backendSyncFlag, false, "whether to do backend fsync")
	root.Flags().Bool(developmentFlag, false, "development mode")
	root.Flags().String(logLevelFlag, "debug", "log level")
	root.Flags().StringSlice(logOutputPathsFlag, nil, "log output path")
}

func Parse(hook Hook) error {
	localHook = hook
	if err := root.Execute(); err != nil {
		return err
	}
	return globalErr
}
