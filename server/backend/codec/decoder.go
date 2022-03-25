package codec

import (
	"bufio"
	"encoding/binary"
	"errors"
	"github.com/vigilglc/raft-lsm/server/backend/kvpb"
	"io"
)

const (
	decoderBufSize = 2 * 1024 * 1024
)

var (
	ErrTooLargeKV          = errors.New("decoder: kvpb.kv's data is too large to unmarshal")
	ErrKVSnapshotCorrupted = errors.New("decoder: kv snapshot is corrupted")
)

type Decoder struct {
	br        *bufio.Reader
	readBuf   []byte
	uint64Buf []byte
}

func NewDecoder(r io.Reader) *Decoder {
	res := new(Decoder)
	res.br = bufio.NewReaderSize(r, decoderBufSize)
	res.readBuf = make([]byte, decoderBufSize, decoderBufSize)
	res.uint64Buf = make([]byte, uint64Size, uint64Size)
	return res
}

func (d *Decoder) Decode(kv *kvpb.KV) (err error) {
	if _, err := io.ReadFull(d.br, d.uint64Buf); err != nil {
		return ErrKVSnapshotCorrupted
	}
	dLen := readUint64(d.uint64Buf)
	if dLen == 0 {
		return io.EOF
	}
	var buf = d.readBuf
	if dLen > uint64(len(d.readBuf)) {
		defer func() {
			_ = recover()
			err = ErrTooLargeKV
		}()
		buf = make([]byte, dLen, dLen)
	}
	if _, err := io.ReadFull(d.br, buf[:dLen]); err != nil {
		return ErrKVSnapshotCorrupted
	}
	err = kv.Unmarshal(buf[:dLen])
	if err != nil {
		return ErrKVSnapshotCorrupted
	}
	return nil
}

func readUint64(buf []byte) uint64 {
	return binary.BigEndian.Uint64(buf)
}
