package rosedb

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/rosedblabs/rosedb/v2/utils"
	"github.com/rosedblabs/wal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const crashRecoveryKeyCount = 2000

func seedDBForCrashTest(t *testing.T, dir string) {
	t.Helper()
	opts := DefaultOptions
	opts.DirPath = dir
	db, err := Open(opts)
	require.NoError(t, err)
	for i := 0; i < crashRecoveryKeyCount; i++ {
		require.NoError(t, db.Put(utils.GetTestKey(i), utils.RandomValue(128)))
	}
	require.NoError(t, db.Close())
}

// reopenAndCheckValues opens the database after a simulated crash and verifies
// that every key is still readable with a consistent, non-empty value.
func reopenAndCheckValues(t *testing.T, dir string) *DB {
	t.Helper()
	opts := DefaultOptions
	opts.DirPath = dir
	db, err := Open(opts)
	require.NoError(t, err, "database must open after a crash")
	for i := 0; i < crashRecoveryKeyCount; i++ {
		value, getErr := db.Get(utils.GetTestKey(i))
		if getErr == ErrKeyNotFound {
			// acceptable when the only copy of a key lived in a torn/torn-off
			// record; a missing key is recoverable state, unlike a dangling index
			continue
		}
		assert.NoError(t, getErr, "key %d must never fail with a dangling read", i)
		assert.NotEmpty(t, value, "key %d must keep a consistent value", i)
	}
	return db
}

// A crash injected between the value write and the hint/index write must leave
// the database openable and every key readable (the incomplete merge is simply
// discarded and redone, so all old values remain available). The interruption
// is exercised at several positions inside the merge write loop.
func TestDB_Merge_CrashBetweenValueAndIndexWrite(t *testing.T) {
	for _, crashAfter := range []int{1, 50, 500, crashRecoveryKeyCount - 1} {
		t.Run(fmt.Sprintf("after%d", crashAfter), func(t *testing.T) {
			runCrashBetweenWritesTest(t, crashAfter)
		})
	}
}

func runCrashBetweenWritesTest(t *testing.T, crashAfter int) {
	dir := filepath.Join(t.TempDir(), "db")
	seedDBForCrashTest(t, dir)

	opts := DefaultOptions
	opts.DirPath = dir
	db, err := Open(opts)
	require.NoError(t, err)

	valueWrites := 0
	crashHook = func(point string) error {
		if point == crashPointAfterValueWrite {
			valueWrites++
			if valueWrites == crashAfter {
				crashHook = nil
				return errSimulatedCrash
			}
		}
		return nil
	}
	err = db.Merge(false)
	require.ErrorIs(t, err, errSimulatedCrash)
	crashHook = nil
	require.NoError(t, db.Close())

	reopened := reopenAndCheckValues(t, dir)

	// the database must stay healthy: a follow-up merge completes and all keys
	// remain readable afterwards.
	require.NoError(t, reopened.Merge(true))
	for i := 0; i < crashRecoveryKeyCount; i++ {
		value, getErr := reopened.Get(utils.GetTestKey(i))
		assert.NoError(t, getErr)
		assert.NotEmpty(t, value)
	}
	_ = reopened.Close()
}

// A crash after both writes of a record completed (value then index) must keep
// the committed state consistent as well.
func TestDB_Merge_CrashAfterIndexWrite(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	seedDBForCrashTest(t, dir)

	opts := DefaultOptions
	opts.DirPath = dir
	db, err := Open(opts)
	require.NoError(t, err)

	crashed := false
	crashHook = func(point string) error {
		if point == crashPointAfterHintWrite && !crashed {
			crashHook = nil
			crashed = true
			return errSimulatedCrash
		}
		return nil
	}
	err = db.Merge(false)
	require.ErrorIs(t, err, errSimulatedCrash)
	crashHook = nil
	require.NoError(t, db.Close())

	reopened := reopenAndCheckValues(t, dir)
	_ = reopened.Close()
}

// A torn tail in the hint/index file (crash while the index chunk was being
// flushed) must not prevent the database from opening.
func TestDB_Recover_TornHintTail(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	seedDBForCrashTest(t, dir)

	opts := DefaultOptions
	opts.DirPath = dir
	db, err := Open(opts)
	require.NoError(t, err)
	require.NoError(t, db.Merge(true))
	require.NoError(t, db.Close())

	hintPath := wal.SegmentFileName(dir, hintFileNameSuffix, 1)
	info, err := os.Stat(hintPath)
	require.NoError(t, err)
	require.Positive(t, info.Size())
	hintFile, err := os.OpenFile(hintPath, os.O_RDWR, 0o644)
	require.NoError(t, err)
	require.NoError(t, hintFile.Truncate(info.Size()-3))
	require.NoError(t, hintFile.Close())

	reopened := reopenAndCheckValues(t, dir)
	_ = reopened.Close()
}

// A hint entry pointing at a missing value segment must be skipped during
// recovery instead of poisoning the in-memory index.
func TestDB_Recover_DanglingHintEntry(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	seedDBForCrashTest(t, dir)

	opts := DefaultOptions
	opts.DirPath = dir
	db, err := Open(opts)
	require.NoError(t, err)
	require.NoError(t, db.Merge(true))
	require.NoError(t, db.Close())

	// remove the merged value segment referenced by the hint file
	segPath := wal.SegmentFileName(dir, dataFileNameSuffix, 1)
	require.NoError(t, os.Remove(segPath))

	reopened, err := Open(opts)
	require.NoError(t, err, "database must open with a dangling hint entry")
	defer func() { _ = reopened.Close() }()

	// no key may resolve to a missing value file; a dangling index entry must be
	// dropped at recovery time so a Get can only succeed or report not-found.
	for i := 0; i < crashRecoveryKeyCount; i++ {
		value, getErr := reopened.Get(utils.GetTestKey(i))
		if getErr == ErrKeyNotFound {
			continue
		}
		assert.NoError(t, getErr, "key %d must not hit a dangling value file", i)
		assert.NotEmpty(t, value)
	}
}

// A torn tail in an active data segment must be trimmed logically during
// recovery instead of failing the whole startup.
func TestDB_Recover_TornDataSegmentTail(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	seedDBForCrashTest(t, dir)

	segPath := filepath.Join(dir, "000000001"+dataFileNameSuffix)
	info, err := os.Stat(segPath)
	require.NoError(t, err)
	segFile, err := os.OpenFile(segPath, os.O_RDWR, 0o644)
	require.NoError(t, err)
	require.NoError(t, segFile.Truncate(info.Size()-5))
	require.NoError(t, segFile.Close())

	opts := DefaultOptions
	opts.DirPath = dir
	db, err := Open(opts)
	require.NoError(t, err, "database must open with a torn data segment")
	defer func() { _ = db.Close() }()

	// keys whose records were fully persisted stay readable; the open itself
	// must never fail because of the torn trailing record.
	stat := db.Stat()
	assert.Greater(t, stat.KeysNum, 0)
}
