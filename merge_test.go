package rosedb

import (
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/rosedblabs/rosedb/v2/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDB_Merge_1_Empty(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.NoError(t, err)
	defer destroyDB(db)

	err = db.Merge(false)
	assert.NoError(t, err)
}

func TestDB_Merge_2_All_Invalid(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.NoError(t, err)
	defer destroyDB(db)

	for i := 0; i < 100000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(128))
		assert.NoError(t, err)
	}
	for i := 0; i < 100000; i++ {
		err := db.Delete(utils.GetTestKey(i))
		assert.NoError(t, err)
	}

	err = db.Merge(false)
	assert.NoError(t, err)

	_ = db.Close()
	db2, err := Open(options)
	assert.NoError(t, err)
	defer func() {
		_ = db2.Close()
	}()

	stat := db2.Stat()
	assert.Equal(t, 0, stat.KeysNum)
}

func TestDB_Merge_3_All_Valid(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.NoError(t, err)
	defer destroyDB(db)

	for i := 0; i < 100000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(128))
		assert.NoError(t, err)
	}

	err = db.Merge(false)
	assert.NoError(t, err)

	_ = db.Close()
	db2, err := Open(options)
	assert.NoError(t, err)
	defer func() {
		_ = db2.Close()
	}()

	for i := 0; i < 100000; i++ {
		val, err := db2.Get(utils.GetTestKey(i))
		assert.NoError(t, err)
		assert.NotNil(t, val)
	}
}

func TestDB_Merge_4_Twice(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	require.NoError(t, err)
	defer destroyDB(db)

	for i := 0; i < 100000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(128))
		assert.NoError(t, err)
	}

	err = db.Merge(false)
	assert.NoError(t, err)
	err = db.Merge(false)
	assert.NoError(t, err)

	_ = db.Close()
	db2, err := Open(options)
	assert.NoError(t, err)
	defer func() {
		_ = db2.Close()
	}()

	for i := 0; i < 100000; i++ {
		val, err := db2.Get(utils.GetTestKey(i))
		assert.NoError(t, err)
		assert.NotNil(t, val)
	}
}

func TestDB_Merge_5_Mixed(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.NoError(t, err)
	defer destroyDB(db)

	for i := 0; i < 100000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(128))
		assert.NoError(t, err)
	}
	for i := 0; i < 100000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(128))
		assert.NoError(t, err)
	}
	for i := 100000; i < 300000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(128))
		assert.NoError(t, err)
	}
	for i := 100000; i < 200000; i++ {
		err := db.Delete(utils.GetTestKey(i))
		assert.NoError(t, err)
	}

	err = db.Merge(false)
	assert.NoError(t, err)

	_ = db.Close()
	db2, err := Open(options)
	assert.NoError(t, err)
	defer func() {
		_ = db2.Close()
	}()
	stat := db2.Stat()
	assert.Equal(t, 200000, stat.KeysNum)
}

func TestDB_Merge_6_Appending(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.NoError(t, err)
	defer destroyDB(db)

	for i := 0; i < 100000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(128))
		assert.NoError(t, err)
	}
	for i := 0; i < 100000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(128))
		assert.NoError(t, err)
	}
	for i := 100000; i < 300000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(128))
		assert.NoError(t, err)
	}
	for i := 100000; i < 200000; i++ {
		err := db.Delete(utils.GetTestKey(i))
		assert.NoError(t, err)
	}

	wg := sync.WaitGroup{}
	m := sync.Map{}
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			for i := 0; i < 10000; i++ {
				key := utils.GetTestKey(rand.Int())
				m.Store(string(key), struct{}{})
				e := db.Put(key, utils.RandomValue(128))
				assert.NoError(t, e)
			}
		}()
	}

	err = db.Merge(false)
	assert.NoError(t, err)

	wg.Wait()

	_ = db.Close()
	db2, err := Open(options)
	assert.NoError(t, err)
	defer func() {
		_ = db2.Close()
	}()
	stat := db2.Stat()
	var count int
	m.Range(func(key, value any) bool {
		count++
		return true
	})
	assert.Equal(t, 200000+count, stat.KeysNum)
}

func TestDB_Multi_Open_Merge(t *testing.T) {
	options := DefaultOptions
	kvs := make(map[string][]byte)
	for i := 0; i < 5; i++ {
		db, err := Open(options)
		assert.NoError(t, err)

		for i := 0; i < 10000; i++ {
			key := utils.GetTestKey(rand.Int())
			value := utils.RandomValue(128)
			kvs[string(key)] = value
			err = db.Put(key, value)
			assert.NoError(t, err)
		}

		err = db.Merge(false)
		assert.NoError(t, err)
		err = db.Close()
		assert.NoError(t, err)
	}
	db, err := Open(options)
	assert.NoError(t, err)
	defer destroyDB(db)

	for key, value := range kvs {
		v, err := db.Get([]byte(key))
		assert.NoError(t, err)
		assert.Equal(t, value, v)
	}
	assert.Equal(t, len(kvs), db.index.Size())
}

func TestDB_Merge_ReopenAfterDone(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.NoError(t, err)
	defer destroyDB(db)

	kvs := make(map[string][]byte)
	for i := 0; i < 200000; i++ {
		key := utils.GetTestKey(i)
		value := utils.RandomValue(128)
		kvs[string(key)] = value
		err := db.Put(key, value)
		assert.NoError(t, err)
	}

	err = db.Merge(true)
	assert.NoError(t, err)
	_, err = os.Stat(mergeDirPath(options.DirPath))
	assert.True(t, os.IsNotExist(err))

	for key, value := range kvs {
		v, err := db.Get([]byte(key))
		assert.NoError(t, err)
		assert.Equal(t, value, v)
	}
	assert.Equal(t, len(kvs), db.index.Size())
}

func TestDB_Merge_Concurrent_Put(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.NoError(t, err)
	defer destroyDB(db)

	wg := sync.WaitGroup{}
	m := sync.Map{}
	wg.Add(11)
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			for i := 0; i < 10000; i++ {
				key := utils.GetTestKey(rand.Int())
				value := utils.RandomValue(128)
				m.Store(string(key), value)
				e := db.Put(key, value)
				assert.NoError(t, e)
			}
		}()
	}
	go func() {
		defer wg.Done()
		err = db.Merge(true)
		assert.NoError(t, err)
	}()
	wg.Wait()

	_, err = os.Stat(mergeDirPath(options.DirPath))
	assert.True(t, os.IsNotExist(err))

	var count int
	m.Range(func(key, value any) bool {
		v, err := db.Get([]byte(key.(string)))
		assert.NoError(t, err)
		assert.Equal(t, value, v)
		count++
		return true
	})
	assert.Equal(t, count, db.index.Size())
}

// TestDB_AutoMerge_Concurrent_Put enables the auto merge cron task and keeps
// writing while merges are running, all the written data must be preserved.
func TestDB_AutoMerge_Concurrent_Put(t *testing.T) {
	options := DefaultOptions
	options.DirPath = t.TempDir()
	// merge every second to trigger merges frequently during the writes
	options.AutoMergeCronExpr = "*/1 * * * * *"
	db, err := Open(options)
	require.NoError(t, err)

	wg := sync.WaitGroup{}
	m := sync.Map{}
	// preload some data so a merge takes a little while
	for i := 0; i < 20000; i++ {
		key := utils.GetTestKey(i)
		value := utils.RandomValue(128)
		require.NoError(t, db.Put(key, value))
		m.Store(string(key), value)
	}

	stop := make(chan struct{})
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				key := []byte(fmt.Sprintf("key-%d-%d", id, i))
				value := utils.RandomValue(64)
				if err := db.Put(key, value); err != nil {
					return
				}
				m.Store(string(key), value)
			}
		}(i)
	}

	// let the auto merge task run several times during the writes
	time.Sleep(8 * time.Second)
	close(stop)
	wg.Wait()
	require.NoError(t, db.Close())

	// reopen the db and check all the written data
	options.AutoMergeCronExpr = ""
	db2, err := Open(options)
	require.NoError(t, err)
	defer destroyDB(db2)

	var count int
	m.Range(func(key, value any) bool {
		v, err := db2.Get([]byte(key.(string)))
		assert.NoError(t, err)
		assert.Equal(t, value.([]byte), v)
		count++
		return true
	})
	assert.Equal(t, count, db2.index.Size())
}
