package backend

import (
	"github.com/vigilglc/raft-lsm/server/backend/codec"
	"github.com/vigilglc/raft-lsm/server/backend/kvpb"
	"go.uber.org/zap"
	"io"
	"os"
	"path/filepath"
	"testing"
)

var testingBackendDir string

func TestMain(m *testing.M) {
	gopath, set := os.LookupEnv("GOPATH")
	if !set {
		os.Exit(-1)
	}
	testingBackendDir = filepath.Join(gopath, "temp", "testing", "backend")
	if err := os.MkdirAll(testingBackendDir, 666); err != nil {
		os.Exit(-1)
	}
	m.Run()
	_ = os.RemoveAll(testingBackendDir)
}

func TestBackendNormal(t *testing.T) {
	config := Config{Dir: testingBackendDir}
	be, err := OpenBackend(zap.NewExample(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(testingBackendDir) }()
	defer func(be Backend) {
		if err := be.Close(); err != nil {
			t.Fatal(err)
		}
	}(be)
	// put
	kvMs := map[string]string{
		"key0": "val0",
		"key1": "val1",
		"key2": "val2",
	}
	var appIdx uint64 = 1
	for k, v := range kvMs {
		if err := be.Put(appIdx, k, v); err != nil {
			t.Fatal(err)
		}
		appIdx++
	}
	for k, v := range kvMs {
		if iv, err := be.Get(k); err != nil {
			t.Fatal(err)
		} else {
			if iv != v {
				t.Fatalf("backend get: %v, expected: %v, actual: %v", k, v, iv)
			}
		}
	}

}

func TestBackendRange(t *testing.T) {
	config := Config{Dir: testingBackendDir}
	be, err := OpenBackend(zap.NewExample(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(testingBackendDir) }()
	defer func(be Backend) {
		if err := be.Close(); err != nil {
			t.Fatal(err)
		}
	}(be)
	// put
	kvMs := map[string]string{}
	for ch := 'a'; ch <= 'z'; ch++ {
		kvMs[string(ch)] = string(ch)
	}
	var appIdx uint64 = 1
	for k, v := range kvMs {
		if err := be.Put(appIdx, k, v); err != nil {
			t.Fatal(err)
		}
		appIdx++
	}
	testCases := []struct {
		from, to string
		asc      bool
	}{
		{"a", "z", true},
		{"a", "f", false},
		{"a", "b", false},
		{"v", "w", true},
		{"z", "z", true},
	}
	for i, tc := range testCases {
		t.Logf("testCase %v: %v\n", i, tc)
		var lastKey = ""
		kvC, errC, closeC := be.Range(tc.from, tc.to, tc.asc)
		var done bool
		for !done {
			select {
			case err := <-errC:
				t.Fatal(err)
			case kv, ok := <-kvC:
				if !ok {
					done = true
					break
				}
				t.Logf("key: %v, val: %v\n", kv.Key, kv.Val)
				if kvMs[kv.Key] != kv.Val {
					t.Fatalf("backend range key: %v, expected: %v, actual: %v", kv.Key, kvMs[kv.Key], kv.Val)
				}
				if tc.from > kv.Key || kv.Key >= tc.to {
					t.Fatalf("backend key:%v out of range:(%v, %v)", kv.Key, tc.from, tc.to)
				}
				if len(lastKey) != 0 {
					cmpRes := config.GetComparer().Compare([]byte(lastKey), []byte(kv.Key))
					if (tc.asc && cmpRes >= 0) || (!tc.asc && cmpRes <= 0) {
						t.Fatalf("backend range wrong key order")
					}
				}
				lastKey = kv.Key
			}
		}
		close(closeC)
		t.Logf("\n")
	}
}

func TestBackendSnapshot(t *testing.T) {
	config := Config{Dir: testingBackendDir}
	be, err := OpenBackend(zap.NewExample(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(testingBackendDir) }()
	defer func(be Backend) {
		if err := be.Close(); err != nil {
			t.Fatal(err)
		}
	}(be)
	// put
	kvMs := map[string]string{}
	for ch := 'a'; ch <= 'z'; ch++ {
		kvMs[string(ch)] = string(ch)
	}
	var appIdx uint64
	for k, v := range kvMs {
		appIdx++
		if err := be.Put(appIdx, k, v); err != nil {
			t.Fatal(err)
		}
	}
	ai, rc, _, err := be.SnapshotStream()
	if err != nil {
		t.Fatal(err)
	}
	if ai != appIdx {
		t.Fatalf("applied index, expected: %v, actual %v", appIdx, ai)
	}
	var kv = new(kvpb.KV)
	var recvM = map[string]string{}
	dec := codec.NewDecoder(rc)
	for err == nil {
		err = dec.Decode(kv)
		if err != nil {
			break
		}
		recvM[kv.Key] = kv.Val
	}
	if err != io.EOF {
		t.Fatal(err)
	}
	for k, v := range kvMs {
		if recvM[k] != v {
			t.Fatalf("received key: %v, whose value expected: %v, actual: %v", k, v, recvM[k])
		}
	}
}
