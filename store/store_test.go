package store_test

import (
	"errors"
	"io"
	"testing"

	"github.com/jacekolszak/deebee/failing"
	"github.com/jacekolszak/deebee/fake"
	"github.com/jacekolszak/deebee/internal/storetest"
	"github.com/jacekolszak/deebee/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpen(t *testing.T) {
	t.Run("should return error when dir is nil", func(t *testing.T) {
		s, err := store.Open(nil)
		require.Error(t, err)
		assert.Nil(t, s)
	})

	t.Run("should open store with no options", func(t *testing.T) {
		dir := fake.ExistingDir()
		s, err := store.Open(dir)
		require.NoError(t, err)
		assert.NotNil(t, s)
	})

	t.Run("should skip nil option", func(t *testing.T) {
		dir := fake.ExistingDir()
		s, err := store.Open(dir, nil)
		require.NoError(t, err)
		assert.NotNil(t, s)
	})

	t.Run("should return client error when store dir does not exist", func(t *testing.T) {
		s, err := store.Open(fake.MissingDir())
		assert.True(t, store.IsClientError(err))
		assert.Nil(t, s)
	})

	t.Run("should return error when option returned error", func(t *testing.T) {
		dir := fake.ExistingDir()
		expectedError := &testError{}
		option := func(s *store.Store) error {
			return expectedError
		}
		// when
		s, err := store.Open(dir, option)
		// then
		assert.True(t, errors.Is(err, expectedError))
		assert.Nil(t, s)
	})

	t.Run("should return error when Dir.Exists() returns error", func(t *testing.T) {
		dir := failing.Exists(fake.ExistingDir())
		s, err := store.Open(dir)
		assert.Error(t, err)
		assert.Nil(t, s)
	})
}

type testError struct{}

func (e testError) Error() string {
	return "test-error"
}

func TestStore_Reader(t *testing.T) {
	t.Run("should return error when no data was previously saved", func(t *testing.T) {
		s := openStore(t, fake.ExistingDir())
		// when
		reader, err := s.Reader()
		// then
		assert.Nil(t, reader)
		assert.False(t, store.IsClientError(err))
		assert.True(t, store.IsDataNotFound(err))
	})

	t.Run("should return error when Store is failing", func(t *testing.T) {
		dirs := map[string]store.Dir{
			"ListFiles":  failing.ListFiles(fake.ExistingDir()),
			"FileReader": failing.FileReader(fake.ExistingDir()),
		}
		for name, dir := range dirs {

			t.Run(name, func(t *testing.T) {
				s := openStore(t, dir)
				if err := writeDataOrError(s, []byte("data")); err != nil {
					return
				}
				// when
				reader, err := s.Reader()
				// then
				assert.Error(t, err)
				assert.Nil(t, reader)
			})
		}
	})
}

func TestStore_Writer(t *testing.T) {
	t.Run("should return error when Store is failing", func(t *testing.T) {
		dirs := map[string]store.Dir{
			"FileWriter": failing.FileWriter(fake.ExistingDir()),
		}
		for name, dir := range dirs {

			t.Run(name, func(t *testing.T) {
				s := openStore(t, dir)
				// when
				writer, err := s.Writer()
				// then
				assert.Error(t, err)
				assert.Nil(t, writer)
			})
		}
	})
}

func TestReadAfterWrite(t *testing.T) {
	t.Run("should read previously written data", func(t *testing.T) {
		tests := map[string][]byte{
			"empty":      {},
			"data":       []byte("data"),
			"MB of data": makeData(1024*1024, 1),
		}
		for name, data := range tests {

			t.Run(name, func(t *testing.T) {
				s := openStore(t, fake.ExistingDir())
				storetest.WriteData(t, s, data)
				// when
				actual := storetest.ReadData(t, s)
				// then
				assert.Equal(t, data, actual)
			})
		}
	})

	t.Run("after update should read last written data", func(t *testing.T) {
		s := openStore(t, fake.ExistingDir())
		updatedData := "updated"
		storetest.WriteData(t, s, []byte("data"))
		storetest.WriteData(t, s, []byte(updatedData))
		// when
		actual := storetest.ReadData(t, s)
		// then
		assert.Equal(t, updatedData, string(actual))
	})

	t.Run("after two updates should read last written data", func(t *testing.T) {
		s := openStore(t, fake.ExistingDir())
		updatedData := "updated"
		storetest.WriteData(t, s, []byte("data1"))
		storetest.WriteData(t, s, []byte("data2"))
		storetest.WriteData(t, s, []byte(updatedData))
		// when
		actual := storetest.ReadData(t, s)
		// then
		assert.Equal(t, updatedData, string(actual))
	})

	t.Run("should update data using different store instance", func(t *testing.T) {
		dir := fake.ExistingDir()
		s := openStore(t, dir)
		storetest.WriteData(t, s, []byte("data"))

		anotherStore := openStore(t, dir)
		updatedData := "updated"
		storetest.WriteData(t, anotherStore, []byte(updatedData))
		// when
		actual := storetest.ReadData(t, anotherStore)
		// then
		assert.Equal(t, updatedData, string(actual))
	})
}

func TestIntegrityChecker(t *testing.T) {
	t.Run("should use custom DataIntegrityChecker", func(t *testing.T) {
		dir := fake.ExistingDir()
		s, err := store.Open(dir, store.IntegrityChecker(&nullIntegrityChecker{}))
		require.NoError(t, err)
		notExpected := []byte("data")
		storetest.WriteData(t, s, notExpected)
		// when
		corruptAllFiles(dir)
		// then
		data := storetest.ReadData(t, s)
		assert.NotEqual(t, notExpected, data)
	})
}

//  Does not check integrity at all
type nullIntegrityChecker struct{}

func (c *nullIntegrityChecker) DecorateReader(reader io.ReadCloser, name string, readChecksum store.ReadChecksum) io.ReadCloser {
	return reader
}

func (c *nullIntegrityChecker) DecorateWriter(writer io.WriteCloser, name string, writeChecksum store.WriteChecksum) io.WriteCloser {
	return writer
}

func openStore(t *testing.T, dir store.Dir) *store.Store {
	s, err := store.Open(dir)
	require.NoError(t, err)
	return s
}

func writeDataOrError(s *store.Store, data []byte) error {
	writer, err := s.Writer()
	if err != nil {
		return err
	}
	_, err = writer.Write(data)
	if err != nil {
		_ = writer.Close()
		return err
	}
	err = writer.Close()
	if err != nil {
		return err
	}
	return nil
}

func makeData(size int, fillWith byte) []byte {
	data := make([]byte, size)
	for i := 0; i < size; i++ {
		data[i] = fillWith
	}
	return data
}

func corruptAllFiles(dir fake.Dir) {
	files := dir.Files()
	for _, file := range files {
		file.Corrupt()
	}
}
