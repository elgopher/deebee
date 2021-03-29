// (c) 2021 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

package store_test

import (
	"errors"
	"io"
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/jacekolszak/deebee/internal/tests"
	"github.com/jacekolszak/deebee/store"
	otiai10 "github.com/otiai10/copy"
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
		invalidDir := path.Join(tempDir(t), "file")
		touchFile(t, invalidDir)
		// when
		s, err := store.Open(invalidDir)
		// then
		require.Error(t, err)
		assert.Nil(t, s)
	})

	t.Run("by default should create dir if does not exist", func(t *testing.T) {
		dir := path.Join(tempDir(t), "missing")
		_, err := store.Open(dir)
		require.NoError(t, err)
		assert.DirExists(t, dir)
	})

	t.Run("by default should create nested dir if does not exist", func(t *testing.T) {
		dir := path.Join(tempDir(t), "nested", "missing")
		_, err := store.Open(dir)
		require.NoError(t, err)
		assert.DirExists(t, dir)
	})

	t.Run("should return error when dir does not exist and FailWhenMissingDir option was used", func(t *testing.T) {
		dir := path.Join(tempDir(t), "missing")
		s, err := store.Open(dir, store.FailWhenMissingDir)
		require.Error(t, err)
		assert.Nil(t, s)
	})

	t.Run("should accept nil option", func(t *testing.T) {
		s, err := store.Open(tempDir(t), nil)
		require.NoError(t, err)
		assert.NotNil(t, s)
	})

	t.Run("should return error when option returned error", func(t *testing.T) {
		option := func(*store.Store) error {
			return errors.New("error")
		}
		s, err := store.Open(tempDir(t), option)
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

	t.Run("should return error when requested version is corrupted", func(t *testing.T) {
		dir := tests.TempDir(t)
		s, err := store.Open(dir)
		require.NoError(t, err)

		version := writeLargeData(t, s, 999, 1234)
		corruptFiles(t, dir)

		reader, err := s.Reader(store.Time(version.Time))
		require.NoError(t, err)
		defer closeSilently(reader)
		// when
		err = readAllDiscarding(reader, 33)
		// then
		assert.Error(t, err)
	})

	t.Run("should return error when all files are corrupted", func(t *testing.T) {
		dir := tests.TempDir(t)
		s, err := store.Open(dir)
		require.NoError(t, err)
		tests.WriteData(t, s, []byte("data1"))
		tests.WriteData(t, s, []byte("data2"))
		corruptFiles(t, dir)
		// when
		_, err = s.Reader()
		// then
		assert.True(t, store.IsVersionNotFound(err))
	})

	t.Run("should read previous version of data when last one is corrupted", func(t *testing.T) {
		dir := tests.TempDir(t)
		s, err := store.Open(dir)
		require.NoError(t, err)
		firstVersionOfData := []byte("data")
		tests.WriteData(t, s, firstVersionOfData)

		dirCopy := tests.TempDir(t)
		err = otiai10.Copy(dir, dirCopy)
		require.NoError(t, err)

		tests.WriteData(t, s, []byte("second version"))
		corruptFiles(t, dir)             // we don't know which files to corrupt therefore all will be corrupted
		err = otiai10.Copy(dirCopy, dir) // bring back files which were not corrupted
		require.NoError(t, err)
		// when
		data := tests.ReadData(t, s)
		// then
		assert.Equal(t, firstVersionOfData, data)
	})
}

func tempDir(t *testing.T) string {
	dir, err := ioutil.TempDir("", "deebee")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, os.RemoveAll(dir))
	})
	return dir
}

func touchFile(t *testing.T, path string) {
	err := ioutil.WriteFile(path, []byte{}, 0664)
	require.NoError(t, err)
}

func corruptFiles(t *testing.T, dir string) {
	files, err := ioutil.ReadDir(dir)
	require.NoError(t, err)
	for _, file := range files {
		corruptFile(t, path.Join(dir, file.Name()))
	}
}

func corruptFile(t *testing.T, file string) {
	stat, err := os.Lstat(file)
	require.NoError(t, err)
	fileSize := stat.Size()

	f, err := os.OpenFile(file, os.O_RDWR, 0664)
	require.NoError(t, err)
	defer closeSilently(f)

	var i int64
	for i = 0; i < fileSize; i += 1010 {
		corruptSingleByteAt(t, f, i)
	}
}

func corruptSingleByteAt(t *testing.T, f *os.File, offset int64) {
	b := make([]byte, 1)
	_, err := f.ReadAt(b, offset)
	require.NoError(t, err)
	b[0] = b[0] + 1
	_, err = f.WriteAt(b, offset)
	require.NoError(t, err)
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
