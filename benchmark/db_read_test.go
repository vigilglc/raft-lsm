package benchmark

import (
	"github.com/syndtr/goleveldb/leveldb"
	"go.etcd.io/bbolt"
	"testing"
)

// func TestInitDBData(t *testing.T) {
// 	boltDB, err := openBoltDB("boltdb0", false)
// 	if err != nil {
// 		t.Error(err)
// 	}
// 	defer func(boltDB *bbolt.DB) {
// 		err := boltDB.Close()
// 		if err != nil {
// 			t.Error(err)
// 		}
// 	}(boltDB)
// 	levelDB, err := openLevelDB("leveldb0", false)
// 	if err != nil {
// 		t.Error(err)
// 	}
// 	defer func(levelDB *leveldb.DB) {
// 		err := levelDB.Close()
// 		if err != nil {
// 			t.Error(err)
// 		}
// 	}(levelDB)
// 	var count = 100000
// 	for i := 0; i < count; i++ {
// 		key, val := newRandomBytes(8), newRandomBytes(256)
// 		if err := levelDB.Put(key, val, nil); err != nil {
// 			t.Error(err)
// 		}
// 		err = boltDB.Update(func(tx *bbolt.Tx) error {
// 			bu, err := tx.CreateBucketIfNotExists([]byte("bench"))
// 			if err != nil {
// 				return err
// 			}
// 			return bu.Put(key, val)
// 		})
// 		if err != nil {
// 			t.Error(err)
// 		}
// 	}
// }

func BenchmarkDB_BoltDB_Read(b *testing.B) {
	db, err := openBoltDB("boltdb", false)
	if err != nil {
		b.Error(err)
	}
	defer func(db *bbolt.DB) { _ = db.Close() }(db)
	b.ResetTimer()
	defer b.StopTimer()
	for i := 0; i < b.N; i++ {
		err := db.View(func(tx *bbolt.Tx) error {
			bucket := tx.Bucket([]byte("bench"))
			bucket.Get(newRandomBytes(8))
			return nil
		})
		if err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkDB_Concurrent1_BoltDB_Read(b *testing.B) {
	benchmarkDBConcurrentBoltDBRead(b, 1)
}
func BenchmarkDB_Concurrent2_BoltDB_Read(b *testing.B) {
	benchmarkDBConcurrentBoltDBRead(b, 2)
}
func BenchmarkDB_Concurrent4_BoltDB_Read(b *testing.B) {
	benchmarkDBConcurrentBoltDBRead(b, 4)
}
func BenchmarkDB_Concurrent8_BoltDB_Read(b *testing.B) {
	benchmarkDBConcurrentBoltDBRead(b, 8)
}
func benchmarkDBConcurrentBoltDBRead(b *testing.B, parallelism int) {
	db, err := openBoltDB("boltdb", false)
	if err != nil {
		b.Error(err)
	}
	defer func(db *bbolt.DB) { _ = db.Close() }(db)
	b.SetParallelism(parallelism)
	b.ResetTimer()
	defer b.StopTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			err := db.View(func(tx *bbolt.Tx) error {
				bucket := tx.Bucket([]byte("bench"))
				bucket.Get(newRandomBytes(8))
				return nil
			})
			if err != nil {
				b.Error(err)
			}
		}
	})
}

func BenchmarkDB_LevelDB_Read(b *testing.B) {
	db, err := openLevelDB("leveldb", false)
	if err != nil {
		b.Error(err)
	}
	defer func(db *leveldb.DB) { _ = db.Close() }(db)
	b.ResetTimer()
	defer b.StopTimer()
	for i := 0; i < b.N; i++ {
		_, _ = db.Get(newRandomBytes(8), nil)
		if err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkDB_Concurrent1_LevelDB_Read(b *testing.B) {
	benchmarkDBConcurrentLevelDBRead(b, 1)
}
func BenchmarkDB_Concurrent2_LevelDB_Read(b *testing.B) {
	benchmarkDBConcurrentLevelDBRead(b, 2)
}
func BenchmarkDB_Concurrent4_LevelDB_Read(b *testing.B) {
	benchmarkDBConcurrentLevelDBRead(b, 4)
}
func BenchmarkDB_Concurrent8_LevelDB_Read(b *testing.B) {
	benchmarkDBConcurrentLevelDBRead(b, 8)
}
func benchmarkDBConcurrentLevelDBRead(b *testing.B, parallelism int) {
	db, err := openLevelDB("leveldb", false)
	if err != nil {
		b.Error(err)
	}
	defer func(db *leveldb.DB) { _ = db.Close() }(db)
	b.SetParallelism(parallelism)
	b.ResetTimer()
	defer b.StopTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = db.Get(newRandomBytes(8), nil)
			if err != nil {
				b.Error(err)
			}
		}
	})
}
