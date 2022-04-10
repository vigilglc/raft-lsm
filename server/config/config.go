package config

import (
	_ "embed"
	"fmt"
	"github.com/BurntSushi/toml"
	json "github.com/json-iterator/go"
	"github.com/vigilglc/raft-lsm/server/backend"
	"github.com/vigilglc/raft-lsm/server/cluster"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type ServerConfig struct {
	// cluster address infos
	ClusterName   string             `json:"clusterName" toml:"clusterName"`
	NewCluster    bool               `json:"newCluster,omitempty" toml:"newCluster"` // whether the cluster is upon creating
	LocalAddrInfo cluster.AddrInfo   `json:"localAddrInfo" toml:"localAddrInfo"`
	PeerAddrInfos []cluster.AddrInfo `json:"peerAddrInfos,omitempty" toml:"peerAddrInfos"`
	// raft and storage
	DataDir           string `json:"dataDir" toml:"dataDir"`     // root dir for storing any data of server.
	OneTickMs         uint16 `json:"oneTickMs" toml:"oneTickMs"` // how many microseconds one tick lasts...
	ElectionTicks     int    `json:"electionTicks" toml:"electionTicks"`
	HeartbeatTicks    int    `json:"heartbeatTicks" toml:"heartbeatTicks"`
	BackendSync       bool   `json:"backendSync,omitempty" toml:"backendSync"`             // whether Backend does fsync...
	BackendForceClose bool   `json:"backendForceClose,omitempty" toml:"backendForceClose"` // if true, Backend.Close will interrupt any in-flight writes...
	SnapshotThreshold uint64 `json:"snapshotThreshold" toml:"snapshotThreshold"`
	// logger
	lg             *zap.Logger
	Development    bool     `json:"development,omitempty" toml:"development"`
	LogLevel       string   `json:"logLevel,omitempty" toml:"logLevel"`
	LogOutputPaths []string `json:"logOutputPaths,omitempty" toml:"logOutputPaths"`
	// req and resp
	ReadIndexBatchTimeoutMs int64 `json:"readIndexBatchTimeoutMs" toml:"readIndexBatchTimeoutMs"`
}

var strMapZapLevel = map[string]zapcore.Level{
	"debug": zapcore.DebugLevel,
	"info":  zapcore.InfoLevel,
	"warn":  zapcore.WarnLevel,
	"error": zapcore.ErrorLevel,
	"panic": zap.PanicLevel,
	"fatal": zap.FatalLevel,
}

func Validate(cfg *ServerConfig) error {
	if cluster.AddInfoEmpty(cfg.LocalAddrInfo) {
		return fmt.Errorf("server config: local addr info not supplied")
	}
	if cfg.NewCluster {
		var err = fmt.Errorf("server config: peer addr info not supplied")
		if len(cfg.PeerAddrInfos) == 0 {
			return err
		}
		for _, pi := range cfg.PeerAddrInfos {
			if cluster.AddInfoEmpty(pi) {
				return fmt.Errorf("server config: %v", err)
			}
		}
	}
	if len(strings.TrimSpace(cfg.DataDir)) == 0 {
		return fmt.Errorf("server config: empty data directory supplied")
	}
	if _, err := filepath.Abs(cfg.DataDir); err != nil {
		return err
	}
	if cfg.OneTickMs == 0 || cfg.ElectionTicks == 0 || cfg.HeartbeatTicks == 0 {
		return fmt.Errorf("server config: values for oneTickMs, electionTicks, heartbeatTicks should be positive")
	}
	if cfg.SnapshotThreshold == 0 {
		return fmt.Errorf("server config: value for snapshotThreshold should be positive")
	}
	if cfg.ReadIndexBatchTimeoutMs == 0 {
		return fmt.Errorf("server config: value for readIndexBatchTimeoutMs should be positive")
	}
	return nil
}

func (cfg *ServerConfig) GetLogger() *zap.Logger {
	return cfg.lg
}

func (cfg *ServerConfig) MakeLogger() {
	if cfg.lg != nil {
		return
	}
	var err error
	logLevel := zapcore.InfoLevel
	if lv, ok := strMapZapLevel[strings.ToLower(cfg.LogLevel)]; ok {
		logLevel = lv
	}
	logOutputPaths := cfg.LogOutputPaths
	if len(logOutputPaths) == 0 {
		logOutputPaths = []string{"stderr"}
	}
	cfg.lg, err = zap.Config{
		Level:       zap.NewAtomicLevelAt(logLevel),
		Development: cfg.Development,
		Sampling: &zap.SamplingConfig{
			Initial:    100,
			Thereafter: 100,
		},
		Encoding:         "json",
		EncoderConfig:    zap.NewProductionEncoderConfig(),
		OutputPaths:      logOutputPaths,
		ErrorOutputPaths: logOutputPaths,
	}.Build()
	if err != nil {
		panic("server config failed to create logger")
	}
}

func (cfg *ServerConfig) GetDataDir() string {
	return cfg.DataDir
}

func (cfg *ServerConfig) MakeDataDir() {
	if err := os.MkdirAll(cfg.GetDataDir(), 0666); err != nil {
		cfg.GetLogger().Fatal("failed to make DataDir",
			zap.String("data-dir", cfg.GetDataDir()), zap.Error(err),
		)
	}
}

func (cfg *ServerConfig) GetWALDir() string {
	return filepath.Join(cfg.GetDataDir(), "storage/wal")
}

func (cfg *ServerConfig) MakeWALDir() {
	if err := os.MkdirAll(cfg.GetWALDir(), 0666); err != nil {
		cfg.GetLogger().Fatal("failed to make WALDir",
			zap.String("WAL-dir", cfg.GetWALDir()), zap.Error(err),
		)
	}
}

func (cfg *ServerConfig) GetSnapshotterDir() string {
	return filepath.Join(cfg.GetDataDir(), "storage/snap")
}

func (cfg *ServerConfig) MakeSnapshotterDir() {
	if err := os.MkdirAll(cfg.GetSnapshotterDir(), 0666); err != nil {
		cfg.GetLogger().Fatal("failed to make WALDir",
			zap.String("WAL-dir", cfg.GetSnapshotterDir()), zap.Error(err),
		)
	}
}

func (cfg *ServerConfig) GetBackendDir() string {
	return filepath.Join(cfg.GetDataDir(), "backend")
}

func (cfg *ServerConfig) GetBackendConfig() backend.Config {
	return backend.Config{
		Dir:        cfg.GetBackendDir(),
		ForceClose: cfg.BackendForceClose,
		Sync:       cfg.BackendSync,
	}
}

func (cfg *ServerConfig) GetOneTickMillis() time.Duration {
	return time.Duration(cfg.OneTickMs) * time.Millisecond
}

func (cfg *ServerConfig) GetRequestTimeout() time.Duration {
	// 5s for queue waiting, computation and disk IO delay
	// + 2 * election timeout for possible leader election
	return 5*time.Second + 2*time.Duration(cfg.ElectionTicks*int(cfg.OneTickMs))*time.Millisecond
}

func (cfg *ServerConfig) GetPeerDialTimeout() time.Duration {
	// 1s for queue wait and election timeout
	return time.Second + time.Duration(cfg.ElectionTicks*int(cfg.OneTickMs))*time.Millisecond
}

//go:embed default.toml
var configToml string

func DefaultServerConfig() *ServerConfig {
	var ret = new(ServerConfig)
	_, err := toml.Decode(configToml, &ret)
	if err != nil {
		panic("failed to decode default config in .toml")
	}
	return ret
}

const (
	configKeyClusterName             = "clusterName"
	configKeyNewCluster              = "newCluster"
	configKeyLocalAddrInfo           = "localAddrInfo"
	configKeyPeerAddrInfos           = "peerAddrInfos"
	configKeyDataDir                 = "dataDir"
	configKeyOneTickMs               = "oneTickMs"
	configKeyElectionTicks           = "electionTicks"
	configKeyHeartbeatTicks          = "heartbeatTicks"
	configKeyBackendSync             = "backendSync"
	configKeyBackendForceClose       = "backendForceClose"
	configKeySnapshotThreshold       = "snapshotThreshold"
	configKeyDevelopment             = "development"
	configKeyLogLevel                = "logLevel"
	configKeyLogOutputPaths          = "logOutputPaths"
	configKeyReadIndexBatchTimeoutMs = "readIndexBatchTimeoutMs"
)

var (
	configKeyNames = [...]string{
		configKeyClusterName,
		configKeyNewCluster,
		configKeyLocalAddrInfo,
		configKeyPeerAddrInfos,
		configKeyDataDir,
		configKeyOneTickMs,
		configKeyElectionTicks,
		configKeyHeartbeatTicks,
		configKeyBackendSync,
		configKeyBackendForceClose,
		configKeySnapshotThreshold,
		configKeyDevelopment,
		configKeyLogLevel,
		configKeyLogOutputPaths,
		configKeyReadIndexBatchTimeoutMs,
	}
)

func mergeTomlConfigs(base, incr *ServerConfig, meta toml.MetaData) {
	for _, name := range configKeyNames {
		if meta.IsDefined(name) {
			switch name {
			case configKeyClusterName:
				base.ClusterName = incr.ClusterName
			case configKeyNewCluster:
				base.NewCluster = incr.NewCluster
			case configKeyLocalAddrInfo:
				base.LocalAddrInfo = incr.LocalAddrInfo
			case configKeyPeerAddrInfos:
				base.PeerAddrInfos = incr.PeerAddrInfos
			case configKeyDataDir:
				base.DataDir = incr.DataDir
			case configKeyOneTickMs:
				base.OneTickMs = incr.OneTickMs
			case configKeyElectionTicks:
				base.ElectionTicks = incr.ElectionTicks
			case configKeyHeartbeatTicks:
				base.HeartbeatTicks = incr.HeartbeatTicks
			case configKeyBackendSync:
				base.BackendSync = incr.BackendSync
			case configKeyBackendForceClose:
				base.BackendForceClose = incr.BackendForceClose
			case configKeySnapshotThreshold:
				base.SnapshotThreshold = incr.SnapshotThreshold
			case configKeyDevelopment:
				base.Development = incr.Development
			case configKeyLogLevel:
				base.LogLevel = incr.LogLevel
			case configKeyLogOutputPaths:
				base.LogOutputPaths = incr.LogOutputPaths
			case configKeyReadIndexBatchTimeoutMs:
				base.ReadIndexBatchTimeoutMs = incr.ReadIndexBatchTimeoutMs
			}
		}
	}
}

func ReadServerConfig(fn string) (ret *ServerConfig, err error) {
	ret = DefaultServerConfig()
	bytes, err := ioutil.ReadFile(fn)
	if err != nil {
		return ret, err
	}
	switch filepath.Ext(fn) {
	case ".json":
		err = json.Unmarshal(bytes, ret)
	case ".toml":
		local := &ServerConfig{}
		meta, err := toml.Decode(string(bytes), local)
		mergeTomlConfigs(ret, local, meta)
		if err != nil {
			return ret, err
		}
	default:
		return ret, fmt.Errorf("unsupported extension: %v", filepath.Ext(fn))
	}
	return
}
