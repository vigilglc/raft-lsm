package fileutil

import (
	"os"
	"path/filepath"
)

func RemovedMatchedUnder(dir string, predf func(dirEnt os.DirEntry) bool) (err error) {
	dirEnts, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, de := range dirEnts {
		var er error
		if predf(de) {
			if de.IsDir() {
				er = os.RemoveAll(filepath.Join(dir, de.Name()))
			} else {
				er = os.Remove(filepath.Join(dir, de.Name()))
			}
		}
		if err == nil {
			err = er
		}
	}
	return err
}
