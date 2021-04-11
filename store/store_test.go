// (c) 2021 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

package store_test

import (
	"errors"
	"io"
	"path"
	"testing"

	"github.com/jacekolszak/deebee/internal/tests"
	"github.com/jacekolszak/deebee/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpen(t *testing.T) {

	t.Run("should return error when dir is empty", func(t *testing.T) {
		s, err := store.Open("")
		require.Error(t, err)
		assert.Nil(t, s)
	})

	t.Run("should return error when dir is not a directory", func(t *testing.T) {
		invalidDir := path.Join(tests.TempDir(t), "file")
		tests.TouchFile(t, invalidDir)
		// when
		s, err := store.Open(invalidDir)
		// then
		require.Error(t, err)
		assert.Nil(t, s)
	})

	t.Run("by default should create dir if does not exist", func(t *testing.T) {
		dir := path.Join(tests.TempDir(t), "missing")
		_, err := store.Open(dir)
		require.NoError(t, err)
		assert.DirExists(t, dir)
	})

	t.Run("by default should create nested dir if does not exist", func(t *testing.T) {
		dir := path.Join(tests.TempDir(t), "nested", "missing")
		_, err := store.Open(dir)
		require.NoError(t, err)
		assert.DirExists(t, dir)
	})

	t.Run("should return error when dir does not exist and FailWhenMissingDir option was used", func(t *testing.T) {
		dir := path.Join(tests.TempDir(t), "missing")
		s, err := store.Open(dir, store.FailWhenMissingDir)
		require.Error(t, err)
		assert.Nil(t, s)
	})

	t.Run("should accept nil option", func(t *testing.T) {
		s, err := store.Open(tests.TempDir(t), nil)
		require.NoError(t, err)
		assert.NotNil(t, s)
	})

	t.Run("should return error when option returned error", func(t *testing.T) {
		option := func(*store.Store) error {
			return errors.New("error")
		}
		s, err := store.Open(tests.TempDir(t), option)
		assert.Error(t, err)
		assert.Nil(t, s)
	})
}

func TestReadAfterWrite(t *testing.T) {

	t.Run("should read previously written data", func(t *testing.T) {
		s := tests.OpenStore(t)
		data := []byte("data")
		tests.WriteData(t, s, data)
		dataRead := tests.ReadData(t, s)
		assert.Equal(t, data, dataRead)
	})

	t.Run("should read previously written data using 2 write operations", func(t *testing.T) {
		s := tests.OpenStore(t)

		writer, _ := s.Writer()
		_, err := writer.Write([]byte("data1"))
		require.NoError(t, err)
		_, err = writer.Write([]byte(" and data2"))
		require.NoError(t, err)

		err = writer.Close()
		require.NoError(t, err)
		// when
		dataRead := tests.ReadData(t, s)
		// then
		assert.Equal(t, []byte("data1 and data2"), dataRead)
	})

	t.Run("should read updated data", func(t *testing.T) {
		s := tests.OpenStore(t)
		newData := []byte("new")
		tests.WriteData(t, s, []byte("old data"))
		tests.WriteData(t, s, newData)
		dataRead := tests.ReadData(t, s)
		assert.Equal(t, newData, dataRead)
	})

	t.Run("when Close was not called, version should not be available to read", func(t *testing.T) {
		s := tests.OpenStore(t)
		writer, _ := s.Writer()
		defer closeSilently(writer)
		// when
		_, err := writer.Write([]byte("data"))
		require.NoError(t, err)
		// then
		_, err = s.Reader()
		assert.True(t, store.IsVersionNotFound(err))
		// and
		versions, err := s.Versions()
		require.NoError(t, err)
		assert.Empty(t, versions)
	})

	t.Run("should return error when version is corrupted", func(t *testing.T) {
		dir := tests.TempDir(t)
		s, err := store.Open(dir)
		require.NoError(t, err)

		version := writeLargeData(t, s, 999, 1234)
		tests.CorruptFiles(t, dir)

		cases := map[string]func() (store.Reader, error){
			"specific version": func() (store.Reader, error) {
				return s.Reader(store.Time(version.Time))
			},
			"latest version": func() (store.Reader, error) {
				return s.Reader()
			},
		}

		for name, openReader := range cases {

			t.Run(name, func(t *testing.T) {
				reader, err := openReader()
				require.NoError(t, err)
				defer closeSilently(reader)
				// when
				err = readAllDiscarding(reader, 33)
				// then
				assert.Error(t, err)
			})
		}

	})

	t.Run("store.NoIntegrityCheck should skip integrity check", func(t *testing.T) {
		dir := tests.TempDir(t)
		s, err := store.Open(dir, store.NoIntegrityCheck)
		require.NoError(t, err)

		tests.WriteData(t, s, []byte("to be corrupted"))
		tests.CorruptFiles(t, dir)

		reader, err := s.Reader()
		require.NoError(t, err)
		// expect
		assertNotCorrupted(t, reader)
	})

	t.Run("updating .sum file with ALTERED should disable integrity check", func(t *testing.T) {
		for _, alteredHash := range []string{"ALTERED", "ALTERED\n", "ALTERED\r\n"} {
			t.Run(alteredHash, func(t *testing.T) {
				dir := tests.TempDir(t)
				s, err := store.Open(dir)
				require.NoError(t, err)

				tests.WriteData(t, s, []byte("to be corrupted"))
				tests.CorruptFiles(t, dir)

				tests.UpdateFiles(t, dir, ".sum", alteredHash)

				reader, err := s.Reader()
				require.NoError(t, err)
				// expect
				assertNotCorrupted(t, reader)
			})
		}
	})
}

func assertNotCorrupted(t *testing.T, reader store.Reader) {
	err1 := readAllDiscarding(reader, 8)
	err2 := reader.Close()
	assert.NoError(t, err1)
	assert.NoError(t, err2)
}

func writeLargeData(t *testing.T, s *store.Store, blocks, blockSize int, writerOptions ...store.WriterOption) store.Version {
	var b byte

	writer, err := s.Writer(writerOptions...)
	require.NoError(t, err)

	for i := 0; i < blocks; i++ {
		block := newBlockOfData(blockSize, b)
		_, err = writer.Write(block)
		require.NoError(t, err)

		lastByteValue := block[len(block)-1]
		b = lastByteValue
	}
	err = writer.Close()
	require.NoError(t, err)
	return writer.Version()
}

// newBlockOfData generates block of data with each byte value higher than previous one by 1
func newBlockOfData(blockSize int, firstByteValue byte) []byte {
	b := firstByteValue
	block := make([]byte, blockSize)
	for j := 0; j < blockSize; j++ {
		block[j] = b
		b++
	}
	return block
}

func readAllDiscarding(reader io.Reader, blockSize int) error {
	block := make([]byte, blockSize)
	for {
		_, err := reader.Read(block)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}
