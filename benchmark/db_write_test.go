package benchmark

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"go.etcd.io/bbolt"
	"os"
	"path/filepath"
	"testing"
)

func openLevelDB(name string, clean bool) (db *leveldb.DB, err error) {
	gopath, set := os.LookupEnv("GOPATH")
	if !set {
		os.Exit(-1)
	}
	dbDir := filepath.Join(gopath, "temp", "benchmark", name)
	if clean {
		_ = os.RemoveAll(dbDir)
	}
	if err := os.MkdirAll(dbDir, 0666); err != nil {
		os.Exit(-1)
	}
	return leveldb.OpenFile(dbDir, &opt.Options{
		NoSync:       false,
		NoWriteMerge: true,
	})
}

func openBoltDB(name string, clean bool) (db *bbolt.DB, err error) {
	gopath, set := os.LookupEnv("GOPATH")
	if !set {
		os.Exit(-1)
	}
	dbDir := filepath.Join(gopath, "temp", "benchmark", name)
	if clean {
		_ = os.RemoveAll(dbDir)
	}
	if err := os.MkdirAll(dbDir, 0666); err != nil {
		os.Exit(-1)
	}
	return bbolt.Open(filepath.Join(dbDir, "bolt.db"), 0666, &bbolt.Options{
		NoSync:     false,
		NoGrowSync: false,
	})
}

func BenchmarkDB_Batch1_BoltDB_Write(b *testing.B) {
	db, err := openBoltDB("boltdb", false)
	if err != nil {
		b.Error(err)
	}
	defer func(db *bbolt.DB) { _ = db.Close() }(db)
	benchmarkBoltDBWriteBatch(b, db, 1)
}

func BenchmarkDB_Batch2_BoltDB_Write(b *testing.B) {
	db, err := openBoltDB("boltdb", false)

	if err != nil {
		b.Error(err)
	}
	defer func(db *bbolt.DB) { _ = db.Close() }(db)
	benchmarkBoltDBWriteBatch(b, db, 2)
}

func BenchmarkDB_Batch5_BoltDB_Write(b *testing.B) {
	db, err := openBoltDB("boltdb", false)

	if err != nil {
		b.Error(err)
	}
	defer func(db *bbolt.DB) { _ = db.Close() }(db)
	benchmarkBoltDBWriteBatch(b, db, 5)
}

func benchmarkBoltDBWriteBatch(b *testing.B, db *bbolt.DB, batch int) {
	b.ResetTimer()
	defer b.StopTimer()
	for i := 0; i < b.N; i++ {
		err := db.Update(func(tx *bbolt.Tx) error {
			bucket, err := tx.CreateBucketIfNotExists([]byte("bench"))
			if err != nil {
				return err
			}
			for i := 0; i < batch; i++ {
				if err := bucket.Put(newRandomBytes(8), newRandomBytes(256)); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkDB_Batch1_LevelDB_Write(b *testing.B) {
	db, err := openLevelDB("leveldb", false)
	if err != nil {
		b.Error(err)
	}
	defer func(db *leveldb.DB) { _ = db.Close() }(db)
	benchmarkLevelDBWriteBatch(b, db, 1)
}

func BenchmarkDB_Batch2_LevelDB_Write(b *testing.B) {
	db, err := openLevelDB("leveldb", false)
	if err != nil {
		b.Error(err)
	}
	defer func(db *leveldb.DB) { _ = db.Close() }(db)
	benchmarkLevelDBWriteBatch(b, db, 2)
}

func BenchmarkDB_Batch5_LevelDB_Write(b *testing.B) {
	db, err := openLevelDB("leveldb", false)
	if err != nil {
		b.Error(err)
	}
	defer func(db *leveldb.DB) { _ = db.Close() }(db)
	benchmarkLevelDBWriteBatch(b, db, 5)
}

func benchmarkLevelDBWriteBatch(b *testing.B, db *leveldb.DB, batch int) {
	b.ResetTimer()
	defer b.StopTimer()
	for i := 0; i < b.N; i++ {
		bat := new(leveldb.Batch)
		for i := 0; i < batch; i++ {
			bat.Put(newRandomBytes(8), newRandomBytes(256))
		}
		err := db.Write(bat, nil)
		if err != nil {
			b.Error(err)
		}
	}
}
