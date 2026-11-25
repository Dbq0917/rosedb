package rosedb

import (
	"testing"

	"github.com/rosedblabs/rosedb/v2/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDB_CommittedTxns_ReclaimedAfterMerge commits many batches, then checks
// that the committed transaction table is rebuilt after the merge has
// replaced the old WAL segments.
func TestDB_CommittedTxns_ReclaimedAfterMerge(t *testing.T) {
	options := DefaultOptions
	options.DirPath = t.TempDir()
	options.SegmentSize = MB

	db, err := Open(options)
	require.NoError(t, err)

	keyNum := 1_000_000
	if testing.Short() {
		keyNum = 100_000
	}
	for i := 0; i < keyNum; i++ {
		require.NoError(t, db.Put(utils.GetTestKey(i), []byte("v")))
	}
	assert.Equal(t, keyNum, len(db.committedTxns))
	assert.Equal(t, keyNum, db.Stat().KeysNum)

	require.NoError(t, db.Merge(true))
	assert.Empty(t, db.committedTxns)
	assert.Equal(t, keyNum, db.Stat().KeysNum)
	require.NoError(t, db.Close())

	// Reopen after the merge: all committed records must still be indexed,
	// and no stale transaction ids should be loaded.
	db2, err := Open(options)
	require.NoError(t, err)
	defer destroyDB(db2)

	assert.Empty(t, db2.committedTxns)
	assert.Equal(t, keyNum, db2.Stat().KeysNum)
	for i := 0; i < keyNum; i += keyNum / 1024 {
		value, err := db2.Get(utils.GetTestKey(i))
		require.NoError(t, err)
		assert.Equal(t, []byte("v"), value)
	}
}

// TestDB_CommittedTxns_KeptUntilSegmentsReplaced verifies that ids are not
// dropped while their original WAL segments still exist, even if a merge has
// already prepared its output files.
func TestDB_CommittedTxns_KeptUntilSegmentsReplaced(t *testing.T) {
	options := DefaultOptions
	options.DirPath = t.TempDir()
	options.SegmentSize = 64 * KB

	const keyNum = 1000
	db, err := Open(options)
	require.NoError(t, err)
	for i := 0; i < keyNum; i++ {
		require.NoError(t, db.Put(utils.GetTestKey(i), []byte("v")))
	}
	assert.Equal(t, keyNum, len(db.committedTxns))

	require.NoError(t, db.Merge(false))
	// The old segments are still the live data files, so their transaction
	// ids are still needed.
	assert.Equal(t, keyNum, len(db.committedTxns))
	assert.Equal(t, keyNum, db.Stat().KeysNum)

	// Writes made after the rotation go into a segment that survives the
	// merge. Their ids must be kept when the database is reopened.
	for i := keyNum; i < keyNum+100; i++ {
		require.NoError(t, db.Put(utils.GetTestKey(i), []byte("v")))
	}
	assert.Equal(t, keyNum+100, len(db.committedTxns))
	require.NoError(t, db.Close())

	// Opening the database installs the completed merge output and replaces
	// the old segments. At that point the old ids can be reclaimed safely.
	db2, err := Open(options)
	require.NoError(t, err)
	defer destroyDB(db2)

	assert.Equal(t, 100, len(db2.committedTxns))
	assert.Equal(t, keyNum+100, db2.Stat().KeysNum)
	for i := 0; i < keyNum+100; i++ {
		value, err := db2.Get(utils.GetTestKey(i))
		require.NoError(t, err)
		assert.Equal(t, []byte("v"), value)
	}
}

// TestDB_CommittedTxns_RebuiltOnOpen makes sure a restart keeps the ids that
// are still referenced by the WAL, otherwise committed records would be
// treated as uncommitted by the next recovery.
func TestDB_CommittedTxns_RebuiltOnOpen(t *testing.T) {
	options := DefaultOptions
	options.DirPath = t.TempDir()
	options.SegmentSize = 64 * KB

	const keyNum = 1000
	db, err := Open(options)
	require.NoError(t, err)
	for i := 0; i < keyNum; i++ {
		require.NoError(t, db.Put(utils.GetTestKey(i), []byte("v")))
	}
	require.NoError(t, db.Close())

	db2, err := Open(options)
	require.NoError(t, err)
	defer destroyDB(db2)

	assert.Equal(t, keyNum, len(db2.committedTxns))
	assert.Equal(t, keyNum, db2.Stat().KeysNum)
	for i := 0; i < keyNum; i++ {
		_, err := db2.Get(utils.GetTestKey(i))
		require.NoError(t, err)
	}
}
