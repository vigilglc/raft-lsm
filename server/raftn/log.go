package raftn

import "go.uber.org/zap"

type ZapRaftLogger struct {
	*zap.SugaredLogger
}

func (lg *ZapRaftLogger) Warning(v ...interface{}) {
	lg.Warn(v)
}

func (lg *ZapRaftLogger) Warningf(format string, v ...interface{}) {
	lg.Warnf(format, v)
}
