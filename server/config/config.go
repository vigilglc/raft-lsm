package config

import (
	"github.com/vigilglc/raft-lsm/server/backend"
	"github.com/vigilglc/raft-lsm/server/cluster"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type ServerConfig struct {
	// cluster address infos
	ClusterName   string             `json:"clusterName"`
	NewCluster    bool               `json:"newCluster,omitempty"` // whether the cluster is upon creating
	LocalAddrInfo cluster.AddrInfo   `json:"localAddrInfo"`
	PeerAddrInfos []cluster.AddrInfo `json:"peerAddrInfos,omitempty"`
	// raft and storage
	DataDir           string `json:"dataDir"`   // root dir for storing any data of server.
	OneTickMs         uint16 `json:"oneTickMs"` // how many microseconds one tick lasts...
	ElectionTicks     int    `json:"electionTicks"`
	HeartbeatTicks    int    `json:"heartbeatTicks"`
	BackendSync       bool   `json:"backendSync,omitempty"`       // whether Backend does fsync...
	BackendForceClose bool   `json:"backendForceClose,omitempty"` // if true, Backend.Close will interrupt any in-flight writes...
	SnapshotThreshold uint64 `json:"SnapshotThreshold"`
	// logger
	lgMu           sync.Mutex
	lg             *zap.Logger
	Development    bool     `json:"development,omitempty"`
	LogLevel       string   `json:"logLevel,omitempty"`
	LogOutputPaths []string `json:"logOutputPaths,omitempty"`
	// req and resp
	ReadIndexBatchTimeoutMs int64 `json:"ReadIndexBatchTimeoutMs"`
}

var strMapZapLevel = map[string]zapcore.Level{
	"debug": zapcore.DebugLevel,
	"info":  zapcore.InfoLevel,
	"warn":  zapcore.WarnLevel,
	"error": zapcore.ErrorLevel,
	"panic": zap.PanicLevel,
	"fatal": zap.FatalLevel,
}

func (cfg *ServerConfig) GetLogger() *zap.Logger {
	var err error
	cfg.lgMu.Lock()
	defer cfg.lgMu.Unlock()
	if cfg.lg != nil {
		return cfg.lg
	}
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
	return cfg.lg
}

func (cfg *ServerConfig) GetDataDir() string {
	lg := cfg.GetLogger()
	err := os.MkdirAll(cfg.DataDir, 0666)
	if err != nil {
		lg.Fatal("data dir of server is invalid", zap.Error(err))
	}
	return cfg.DataDir
}

func (cfg *ServerConfig) GetWALDir() string {
	return filepath.Join(cfg.GetDataDir(), "storage/wal")
}

func (cfg *ServerConfig) GetSnapshotterDir() string {
	return filepath.Join(cfg.GetDataDir(), "storage/snap")
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

func (cfg *ServerConfig) GetRequestTimeout() time.Duration {
	// 5s for queue waiting, computation and disk IO delay
	// + 2 * election timeout for possible leader election
	return 5*time.Second + 2*time.Duration(cfg.ElectionTicks*int(cfg.OneTickMs))*time.Millisecond
}
