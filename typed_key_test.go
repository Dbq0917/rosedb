package rosedb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/bytebufferpool"
)

func TestDB_KeyType_ListThenHash(t *testing.T) {
	options := DefaultOptions
	options.DirPath = t.TempDir()
	db, err := Open(options)
	require.NoError(t, err)

	key := []byte("shared-config")
	listValue := []byte(`["one","two"]`)
	require.NoError(t, db.PutWithType(key, listValue, DataTypeList))

	err = db.PutWithType(key, []byte(`{"one":"two"}`), DataTypeHash)
	require.ErrorIs(t, err, ErrWrongType)

	value, err := db.GetWithType(key, DataTypeList)
	require.NoError(t, err)
	assert.Equal(t, listValue, value)

	_, err = db.GetWithType(key, DataTypeHash)
	require.ErrorIs(t, err, ErrWrongType)

	dataType, err := db.GetDataType(key)
	require.NoError(t, err)
	assert.Equal(t, DataTypeList, dataType)

	// A generic Put cannot bypass the type constraint; a typed update can.
	updatedListValue := []byte(`["one","two","three"]`)
	require.ErrorIs(t, db.Put(key, updatedListValue), ErrWrongType)
	value, err = db.GetWithType(key, DataTypeList)
	require.NoError(t, err)
	assert.Equal(t, listValue, value)
	require.NoError(t, db.PutWithType(key, updatedListValue, DataTypeList))

	require.NoError(t, db.Close())
	db, err = Open(options)
	require.NoError(t, err)
	defer destroyDB(db)

	value, err = db.GetWithType(key, DataTypeList)
	require.NoError(t, err)
	assert.Equal(t, updatedListValue, value)
	err = db.PutWithType(key, []byte(`{"one":"three"}`), DataTypeHash)
	require.ErrorIs(t, err, ErrWrongType)
}

func TestDB_KeyType_LegacyRecordCanBeBoundWithoutDataLoss(t *testing.T) {
	options := DefaultOptions
	options.DirPath = t.TempDir()
	db, err := Open(options)
	require.NoError(t, err)

	key := []byte("legacy-config")
	value := []byte(`["legacy","list"]`)
	putUntypedRecord(t, db, key, value)

	dataType, err := db.GetDataType(key)
	require.NoError(t, err)
	assert.Equal(t, DataTypeUntyped, dataType)

	require.NoError(t, db.SetDataType(key, DataTypeList))
	boundValue, err := db.GetWithType(key, DataTypeList)
	require.NoError(t, err)
	assert.Equal(t, value, boundValue)

	err = db.SetDataType(key, DataTypeHash)
	require.ErrorIs(t, err, ErrWrongType)
	err = db.PutWithType(key, []byte(`{}`), DataTypeHash)
	require.ErrorIs(t, err, ErrWrongType)

	require.NoError(t, db.Close())
	db, err = Open(options)
	require.NoError(t, err)
	defer destroyDB(db)

	dataType, err = db.GetDataType(key)
	require.NoError(t, err)
	assert.Equal(t, DataTypeList, dataType)
	boundValue, err = db.Get(key)
	require.NoError(t, err)
	assert.Equal(t, value, boundValue)
}

func TestDB_KeyType_GenericBytesCanBeMigrated(t *testing.T) {
	options := DefaultOptions
	options.DirPath = t.TempDir()
	db, err := Open(options)
	require.NoError(t, err)
	defer destroyDB(db)

	key := []byte("config-written-before-typing")
	value := []byte(`["old","value"]`)
	require.NoError(t, db.Put(key, value))

	dataType, err := db.GetDataType(key)
	require.NoError(t, err)
	assert.Equal(t, DataTypeBytes, dataType)

	require.NoError(t, db.SetDataType(key, DataTypeList))
	boundValue, err := db.GetWithType(key, DataTypeList)
	require.NoError(t, err)
	assert.Equal(t, value, boundValue)
	require.ErrorIs(t, db.PutWithType(key, []byte(`{}`), DataTypeHash), ErrWrongType)
}

func TestDB_KeyType_SurvivesMerge(t *testing.T) {
	options := DefaultOptions
	options.DirPath = t.TempDir()
	db, err := Open(options)
	require.NoError(t, err)
	defer destroyDB(db)

	key := []byte("merged-config")
	value := []byte(`["one","two"]`)
	require.NoError(t, db.PutWithType(key, value, DataTypeList))
	require.NoError(t, db.Merge(true))

	dataType, err := db.GetDataType(key)
	require.NoError(t, err)
	assert.Equal(t, DataTypeList, dataType)
	mergedValue, err := db.GetWithType(key, DataTypeList)
	require.NoError(t, err)
	assert.Equal(t, value, mergedValue)
	require.ErrorIs(t, db.PutWithType(key, []byte(`{}`), DataTypeHash), ErrWrongType)
}

func TestBatch_KeyType_ConflictDoesNotChangePendingValue(t *testing.T) {
	options := DefaultOptions
	options.DirPath = t.TempDir()
	db, err := Open(options)
	require.NoError(t, err)
	defer destroyDB(db)

	key := []byte("batch-config")
	batch := db.NewBatch(DefaultBatchOptions)
	require.NoError(t, batch.PutWithType(key, []byte(`["one"]`), DataTypeList))
	require.ErrorIs(t, batch.PutWithType(key, []byte(`{"one":"two"}`), DataTypeHash), ErrWrongType)

	value, err := batch.GetWithType(key, DataTypeList)
	require.NoError(t, err)
	assert.Equal(t, []byte(`["one"]`), value)
	require.NoError(t, batch.Commit())

	value, err = db.GetWithType(key, DataTypeList)
	require.NoError(t, err)
	assert.Equal(t, []byte(`["one"]`), value)
}

func putUntypedRecord(t *testing.T, db *DB, key, value []byte) {
	t.Helper()

	batch := db.NewBatch(DefaultBatchOptions)
	defer func() {
		_ = batch.Rollback()
	}()

	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)

	record := &LogRecord{Key: key, Value: value, Type: LogRecordNormal}
	position, err := db.dataFiles.Write(encodeLogRecord(record, db.encodeHeader, buf))
	require.NoError(t, err)
	db.index.Put(key, position)
}
