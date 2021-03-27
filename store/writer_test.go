// (c) 2021 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

package store_test

import (
	"errors"
	"testing"
	"time"

	"github.com/jacekolszak/deebee/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStore_Writer(t *testing.T) {

	t.Run("should return writer", func(t *testing.T) {
		s := openStore(t)
		// when
		writer, err := s.Writer()
		// then
		defer closeSilently(writer)
		require.NoError(t, err)
		assert.NotNil(t, writer)
	})

	t.Run("should open writer with specific time", func(t *testing.T) {
		s := openStore(t)
		writeTime := time.Unix(1000, 0)
		// when
		writer, err := s.Writer(store.WriteTime(writeTime))
		// then
		defer closeSilently(writer)
		require.NoError(t, err)
		assert.NotNil(t, writer)
		assert.True(t, writer.Version().Time.Equal(writeTime), "time not equal")
	})

	t.Run("should accept nil option", func(t *testing.T) {
		s := openStore(t)
		w, err := s.Writer(nil)
		defer closeSilently(w)
		require.NoError(t, err)
		assert.NotNil(t, w)
	})

	t.Run("should return error when option returned error", func(t *testing.T) {
		s := openStore(t)
		option := func(*store.WriterOptions) error {
			return errors.New("error")
		}
		w, err := s.Writer(option)
		require.Error(t, err)
		assert.Nil(t, w)
	})

}

func TestWriter_Write(t *testing.T) {

	t.Run("should return length of data and nil error", func(t *testing.T) {
		s := openStore(t)
		writer, _ := s.Writer()
		defer closeSilently(writer)
		data := []byte("data")
		// when
		n, err := writer.Write(data)
		// then
		require.NoError(t, err)
		assert.Equal(t, len(data), n)
	})

	t.Run("consecutive writes should increase version size", func(t *testing.T) {
		s := openStore(t)
		writer, _ := s.Writer()
		defer closeSilently(writer)
		data := []byte("data")
		dataLen := int64(len(data))
		// when
		_, err := writer.Write(data)
		// then
		require.NoError(t, err)
		assert.Equal(t, dataLen, writer.Version().Size)
		// and when
		_, err = writer.Write(data)
		// then
		require.NoError(t, err)
		assert.Equal(t, dataLen*2, writer.Version().Size)
	})
}

func TestWriter_AbortAndClose(t *testing.T) {

	t.Run("aborted data for a new store should not be available for read", func(t *testing.T) {
		s := openStore(t)
		writer, _ := s.Writer()
		_, err := writer.Write([]byte("data"))
		require.NoError(t, err)
		// when
		writer.AbortAndClose()
		// then
		_, err = s.Reader()
		assert.True(t, store.IsVersionNotFound(err))
		// and
		versions, err := s.Versions()
		require.NoError(t, err)
		assert.Empty(t, versions)
	})

	t.Run("aborted data for a store with previously written data should not be available for read", func(t *testing.T) {
		s := openStore(t)
		oldData := []byte("old")
		writeData(t, s, oldData)
		versionsBefore := readVersions(t, s)

		writer, _ := s.Writer()
		_, err := writer.Write([]byte("data which will be aborted"))
		require.NoError(t, err)
		// when
		writer.AbortAndClose()
		// then
		dataRead := readData(t, s)
		assert.Equal(t, oldData, dataRead)
		// and
		versionsAfter := readVersions(t, s)
		assert.Equal(t, versionsBefore, versionsAfter)
	})
}

func TestWriter_Version(t *testing.T) {

	t.Run("should return Version for newly created writer", func(t *testing.T) {
		s := openStore(t)
		writer, _ := s.Writer()
		defer closeSilently(writer)
		// when
		version := writer.Version()
		assert.NotEqual(t, time.Time{}, version.Time, "time not set")
		assert.Equal(t, int64(0), version.Size)
	})
}

func TestWriter_Close(t *testing.T) {

	t.Run("should return error when trying to close already closed writer", func(t *testing.T) {
		s := openStore(t)
		writer, _ := s.Writer()
		_ = writer.Close()
		// when
		err := writer.Close()
		// then
		assert.Error(t, err)
	})

	t.Run("should no sync when closing the file", func(t *testing.T) {
		s := openStore(t)
		writer, _ := s.Writer(store.NoSync)
		// when
		err := writer.Close()
		// then
		require.NoError(t, err)
	})
}

func readVersions(t *testing.T, s *store.Store) []store.Version {
	v, err := s.Versions()
	require.NoError(t, err)
	return v
}
