package backend

import (
	"encoding/json"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb/comparer"
	"github.com/vigilglc/raft-lsm/server/backend/codec"
	"github.com/vigilglc/raft-lsm/server/utils/fileutil"
	"io/ioutil"
	"os"
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
			if dirEnt.Name() == backendMataFilename || dirEnt.Name() == backendMataBackupName {
				return false
			}
		} else {
			if filepath.Base(mainDir) == dirEnt.Name() {
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
		meta, err := readConfigMeta(c.Dir, backendMataBackupName)
		if err != nil {
			meta, err = readConfigMeta(c.Dir, backendMataFilename)
		}
		if err != nil && !os.IsNotExist(err) {
			return "", err
		}
		c.meta = meta
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
const backendMataBackupName = "backend.meta.json.bak"

func writeConfigMeta(dir, fn string, meta Meta) (err error) {
	f, err := os.Create(filepath.Join(dir, fn))
	defer func(f *os.File) { err = f.Close() }(f)
	if err != nil {
		return err
	}
	jsonB, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	if err := codec.WriteFull(f, jsonB); err != nil {
		return err
	}
	return err
}

func readConfigMeta(dir, fn string) (meta Meta, err error) {
	f, err := os.OpenFile(filepath.Join(dir, fn), os.O_RDONLY, 0444)
	defer func(f *os.File) {
		var er error
		if f != nil {
			er = f.Close()
		}
		if err == nil {
			err = er
		}
	}(f)
	if err != nil {
		return
	}
	jsonB, err := ioutil.ReadAll(f)
	if err != nil {
		return
	}
	err = json.Unmarshal(jsonB, &meta)
	if err != nil {
		return
	}
	return
}

func (c *Config) SetMainDir(dir string) error {
	cpMeta := c.meta
	cpMeta.MainSubDirname = filepath.Base(dir)
	if err := writeConfigMeta(c.Dir, backendMataBackupName, cpMeta); err != nil {
		return err
	}
	if err := writeConfigMeta(c.Dir, backendMataFilename, cpMeta); err != nil {
		return err
	}
	c.meta = cpMeta
	_ = os.Remove(filepath.Join(c.Dir, backendMataFilename))
	_ = os.Rename(filepath.Join(c.Dir, backendMataBackupName), filepath.Join(c.Dir, backendMataFilename))
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
