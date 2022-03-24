package codec

import (
	"bytes"
	"github.com/vigilglc/raft-lsm/server/backend/kvpb"
	"io"
	"math"
	"testing"
)

type bytesWriter struct {
	bts []byte
}

func (bts *bytesWriter) Write(p []byte) (n int, err error) {
	bts.bts = append(bts.bts, p...)
	return len(p), nil
}

func (bts *bytesWriter) String() string {
	return string(bts.bts)
}

func TestDecodeNormalData(t *testing.T) {
	var err error
	bysWrt := new(bytesWriter)
	e := NewEncoder(bysWrt)
	dict := [...]kvpb.KV{
		{Key: "Who Are you? ", Val: "1"},
		{Key: "2", Val: "I am fine, thank you, and you? "},
		{Key: "I am fine too! ", Val: "Thank you! "},
	}
	for _, kv := range dict {
		if err = e.Encode(&kv); err != nil {
			t.Fatalf("Encode error: %s", err)
		}
	}
	if err = e.Close(); err != nil {
		t.Fatalf("Encoder Close error: %s", err)
	}
	var dest []kvpb.KV = nil
	d := NewDecoder(bytes.NewReader([]byte(bysWrt.String())))
	var kv = new(kvpb.KV)
	for true {
		err = d.Decode(kv)
		if err != nil {
			break
		}
		dest = append(dest, *kv)
	}
	if err != io.EOF {
		t.Fatalf("decoder decode error: %s", err)
	}
	if len(dict) != len(dest) {
		t.Fatalf("Mismatched len, expected: %d, actual: %d", len(dict), len(dest))
	}
	for i := 0; i < len(dict); i++ {
		if dict[i] != dest[i] {
			t.Fatalf("Malformed KV, expected: %v, actual: %v", dict[i], dest[i])
		}
	}
}

func TestDecodeCorrupted(t *testing.T) {
	var err error
	var kv = new(kvpb.KV)
	end8Bytes := []uint64{0, 1, math.MaxInt64}
	uint8Buf := make([]byte, 8)
	for _, end8B := range end8Bytes {
		writeUint64(end8B, uint8Buf)
		d := NewDecoder(bytes.NewReader(uint8Buf))
		err = d.Decode(kv)
		if err == nil {
			t.Fatalf("Should return err: %v", io.EOF)
		}
	}
}
