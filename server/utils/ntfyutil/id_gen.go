package ntfyutil

import (
	"math"
	"sync/atomic"
	"time"
)

const (
	prefixLen    = 13
	timestampLen = 41
	sequenceLen  = 10
	suffixLen    = timestampLen + sequenceLen
	suffixMask   = math.MaxUint64 >> prefixLen
)

type IDGenerator struct {
	prefix uint64
	suffix uint64
}

func NewIDGenerator(machine uint64, time time.Time) *IDGenerator {
	return &IDGenerator{
		prefix: machine << suffixLen,
		suffix: uint64(time.UnixMilli()) << sequenceLen,
	}
}

func (gen *IDGenerator) Next() uint64 {
	return gen.prefix | (suffixMask & atomic.AddUint64(&gen.suffix, 1))
}
