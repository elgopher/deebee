// (c) 2021 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

package store_test

import (
	"errors"
	"io"
	"testing"
	"time"

	"github.com/jacekolszak/deebee/internal/tests"
	"github.com/jacekolszak/deebee/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStore_Reader(t *testing.T) {

	t.Run("compacter.when no version is found", func(t *testing.T) {
		s := tests.OpenStore(t)
		reader, err := s.Reader()
		require.Error(t, err)
		assert.True(t, store.IsVersionNotFound(err))
		assert.Nil(t, reader)
	})

	t.Run("should return error when specific version is not found", func(t *testing.T) {
		s := tests.OpenStore(t)
		tests.WriteData(t, s, []byte("data"))
		missing := time.Time{}
		// when
		r, err := s.Reader(store.Time(missing))
		// then
		require.Error(t, err)
		assert.True(t, store.IsVersionNotFound(err))
		assert.Nil(t, r)
	})

	t.Run("should read specific version", func(t *testing.T) {
		s := tests.OpenStore(t)
		v1data := []byte("version 1")
		v1 := tests.WriteData(t, s, v1data)
		tests.WriteData(t, s, []byte("version 2"))
		// when
		dataRead := tests.ReadData(t, s, store.Time(v1.Time))
		// then
		assert.Equal(t, v1data, dataRead)
	})

	t.Run("should skip nil option", func(t *testing.T) {
		s := tests.OpenStore(t)
		tests.WriteData(t, s, []byte("data"))
		// when
		r, err := s.Reader(nil)
		defer closeSilently(r)
		// then
		require.NoError(t, err)
		assert.NotNil(t, r)
	})

	t.Run("should return error when option returned error", func(t *testing.T) {
		s := tests.OpenStore(t)
		tests.WriteData(t, s, []byte("data"))
		option := func(*store.ReaderOptions) error {
			return errors.New("error")
		}
		// when
		r, err := s.Reader(option)
		// then
		assert.Error(t, err)
		assert.Nil(t, r)
	})
}

func TestReader_Version(t *testing.T) {

	t.Run("should return version", func(t *testing.T) {
		s := tests.OpenStore(t)
		data := []byte("data")
		dataLen := int64(len(data))
		version := tests.WriteData(t, s, data)
		reader, _ := s.Reader()
		defer closeSilently(reader)
		// when
		versionRead := reader.Version()
		// then
		assert.True(t, version.Time.Equal(versionRead.Time), "time not equal")
		assert.Equal(t, dataLen, versionRead.Size)
	})
}

func TestReader_Close(t *testing.T) {

	t.Run("should return error when trying to close already closed reader", func(t *testing.T) {
		s := tests.OpenStore(t)
		tests.WriteData(t, s, []byte("data"))
		reader, _ := s.Reader()
		_ = reader.Close()
		// when
		err := reader.Close()
		// then
		assert.Error(t, err)
	})

	t.Run("should return error for corrupted file", func(t *testing.T) {
		var (
			dir  = tests.TempDir(t)
			s, _ = store.Open(dir)
			data = []byte("data to be corrupted")
			out  = make([]byte, len(data))
		)
		tests.WriteData(t, s, data)
		tests.CorruptFiles(t, dir)

		reader, err := s.Reader()
		require.NoError(t, err)
		_, err = reader.Read(out) // read into buffer with the same size as file, so no EOF is reported
		require.NoError(t, err)
		// when
		err = reader.Close()
		// then
		assert.Error(t, err)
	})
}

func closeSilently(c io.Closer) {
	_ = c.Close()
}
