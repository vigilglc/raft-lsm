package backend

import (
	"encoding/json"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb/comparer"
	"github.com/vigilglc/raft-lsm/server/backend/codec"
	"github.com/vigilglc/raft-lsm/server/utils/fileutil"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
)

type Meta struct {
	MainSubDirname string
}

type Config struct {
	meta       Meta
	Comparer   comparer.Comparer
	Dir        string
	ForceClose bool
	Sync       bool
}

func (c *Config) GetComparer() comparer.Comparer {
	if c.Comparer == nil {
		return comparer.DefaultComparer
	}
	return c.Comparer
}

func (c *Config) CleanDir() error {
	mainDir, err := c.GetMainDir()
	if err != nil {
		return err
	}
	return fileutil.RemovedMatchedUnder(c.Dir, func(dirEnt os.DirEntry) bool {
		if !dirEnt.IsDir() {
			if dirEnt.Name() == backendMataFilename {
				return false
			}
		} else {
			if path.Base(mainDir) == dirEnt.Name() {
				return false
			}
		}
		return true
	})
}

func (c *Config) MakeDirAll() error {
	return os.MkdirAll(c.Dir, 0666)
}

func (c *Config) GetMainDir() (string, error) {
	if len(c.meta.MainSubDirname) == 0 {
		f, err := openConfigMetaFile(c.Dir)
		if err != nil {
			return "", err
		}
		jsonB, err := ioutil.ReadAll(f)
		if err != nil {
			return "", err
		}
		if err := json.Unmarshal(jsonB, &c.meta); err != nil {
			return "", err
		}
		if err := f.Close(); err != nil {
			return "", err
		}
	}
	if len(c.meta.MainSubDirname) == 0 {
		tempDir, err := c.MakeTempDir(0)
		if err != nil {
			return tempDir, err
		}
		if err := c.SetMainDir(tempDir); err != nil {
			return "", err
		}
	}
	return filepath.Join(c.Dir, c.meta.MainSubDirname), nil
}

const backendMataFilename = "backend.meta.json"

func openConfigMetaFile(dir string) (f *os.File, err error) {
	return os.Create(filepath.Join(dir, backendMataFilename))
}

func (c *Config) SetMainDir(dir string) error {
	f, err := openConfigMetaFile(c.Dir)
	if err != nil {
		return err
	}
	cpMeta := c.meta
	cpMeta.MainSubDirname = filepath.Base(dir)
	jsonB, err := json.Marshal(cpMeta)
	if err != nil {
		return err
	}
	if err := codec.WriteFull(f, jsonB); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	c.meta = cpMeta
	return nil
}

func (c *Config) MakeTempDir(ai uint64) (tmpDir string, err error) {
	return os.MkdirTemp(c.Dir, fmt.Sprintf("backend-%d-*", ai))
}

func (c *Config) GetSync() bool {
	if c == nil {
		return true
	}
	return c.Sync
}

func (c *Config) GetForceClose() bool {
	if c == nil {
		return false
	}
	return c.ForceClose
}
