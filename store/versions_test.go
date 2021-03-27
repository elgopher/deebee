// (c) 2021 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

package store_test

import (
	"testing"
	"time"

	"github.com/jacekolszak/deebee/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStore_Versions(t *testing.T) {

	t.Run("should return empty slice for new store", func(t *testing.T) {
		s := openStore(t)
		versions, err := s.Versions()
		require.NoError(t, err)
		assert.Empty(t, versions)
	})

	t.Run("should add version once writer is closed", func(t *testing.T) {
		s := openStore(t)
		data := []byte("data")
		dataLen := int64(len(data))
		version := writeData(t, s, data)
		// when
		versions, err := s.Versions()
		// then
		require.NoError(t, err)
		require.Len(t, versions, 1)
		assert.Equal(t, dataLen, versions[0].Size)
		assert.True(t, version.Time.Equal(versions[0].Time), "times not equal")
	})

	t.Run("should sort by time, oldest first", func(t *testing.T) {
		t1, err := time.Parse(time.RFC3339, "2001-01-01T17:00:00+02:00") // 16:00 +01:00
		require.NoError(t, err)
		t2, err := time.Parse(time.RFC3339, "2001-01-01T16:30:00+01:00") // 16:30 +01:00
		require.NoError(t, err)
		t3, err := time.Parse(time.RFC3339, "2001-01-01T15:45:00Z") // 16:45 +01:00
		require.NoError(t, err)

		s := openStore(t)
		v1 := writeData(t, s, []byte("1"), store.WriteTime(t1))
		v2 := writeData(t, s, []byte("2"), store.WriteTime(t2))
		v3 := writeData(t, s, []byte("3"), store.WriteTime(t3))
		// when
		versions, err := s.Versions()
		// then
		require.NoError(t, err)
		require.Len(t, versions, 3)
		assert.True(t, v1.Time.Equal(versions[0].Time))
		assert.True(t, v2.Time.Equal(versions[1].Time))
		assert.True(t, v3.Time.Equal(versions[2].Time))
	})
}

func TestStore_DeleteVersion(t *testing.T) {

	t.Run("should return error when version does not exist", func(t *testing.T) {
		s := openStore(t)
		missingVersion := time.Now()
		err := s.DeleteVersion(missingVersion)
		require.Error(t, err)
		assert.True(t, store.IsVersionNotFound(err))
	})

	t.Run("should delete last available version", func(t *testing.T) {
		s := openStore(t)
		version := writeData(t, s, []byte("data"))
		// when
		err := s.DeleteVersion(version.Time)
		// then
		require.NoError(t, err)
		_, err = s.Reader()
		assert.True(t, store.IsVersionNotFound(err))
	})
}

func writeData(t *testing.T, s *store.Store, bytes []byte, writerOptions ...store.WriterOption) store.Version {
	writer, err := s.Writer(writerOptions...)
	require.NoError(t, err)
	_, err = writer.Write(bytes)
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)
	return writer.Version()
}
