package codec

import (
	"bufio"
	"encoding/binary"
	"github.com/vigilglc/raft-lsm/server/backend/kvpb"
	"io"
)

const (
	encoderBufSize = 1024 * 1024
	uint64Size     = 8
)

type Encoder struct {
	bw        *bufio.Writer
	uint64Buf []byte
}

func NewEncoder(w io.Writer) *Encoder {
	res := new(Encoder)
	res.bw = bufio.NewWriterSize(w, encoderBufSize)
	res.uint64Buf = make([]byte, uint64Size, uint64Size)
	return res
}

func (e *Encoder) Encode(kv *kvpb.KV) error {
	data, err := kv.Marshal()
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return nil
	}
	writeUint64(uint64(len(data)), e.uint64Buf)
	if err := WriteFull(e.bw, e.uint64Buf); err != nil {
		return err
	}
	if err := WriteFull(e.bw, data); err != nil {
		return err
	}
	return nil
}

func (e *Encoder) Close() error { // write zero, which is the end of snapshot...
	writeUint64(0, e.uint64Buf)
	if err := WriteFull(e.bw, e.uint64Buf); err != nil {
		return err
	}
	return e.bw.Flush()
}

func writeUint64(ui uint64, buf []byte) {
	binary.BigEndian.PutUint64(buf, ui)
}

func WriteFull(w io.Writer, buf []byte) (err error) {
	var n, nn int
	for n < len(buf) {
		nn, err = w.Write(buf[n:])
		if err != nil {
			return err
		}
		n += nn
	}
	return err
}
