package deebee_test

import (
	"errors"
	"io"
	"io/ioutil"
	"testing"

	"github.com/jacekolszak/deebee"
	"github.com/jacekolszak/deebee/failing"
	"github.com/jacekolszak/deebee/fake"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpen(t *testing.T) {
	t.Run("should return error when dir is nil", func(t *testing.T) {
		db, err := deebee.Open(nil)
		require.Error(t, err)
		assert.Nil(t, db)
	})

	t.Run("should open db with no options", func(t *testing.T) {
		dir := fake.ExistingDir()
		db, err := deebee.Open(dir)
		require.NoError(t, err)
		assert.NotNil(t, db)
	})

	t.Run("should skip nil option", func(t *testing.T) {
		dir := fake.ExistingDir()
		db, err := deebee.Open(dir, nil)
		require.NoError(t, err)
		assert.NotNil(t, db)
	})

	t.Run("should return client error when database dir does not exist", func(t *testing.T) {
		db, err := deebee.Open(fake.MissingDir())
		assert.True(t, deebee.IsClientError(err))
		assert.Nil(t, db)
	})

	t.Run("should return error when option returned error", func(t *testing.T) {
		dir := fake.ExistingDir()
		expectedError := &testError{}
		option := func(db *deebee.DB) error {
			return expectedError
		}
		// when
		db, err := deebee.Open(dir, option)
		// then
		assert.True(t, errors.Is(err, expectedError))
		assert.Nil(t, db)
	})

	t.Run("should return error when Dir.Exists() returns error", func(t *testing.T) {
		dir := failing.Exists(fake.ExistingDir())
		db, err := deebee.Open(dir)
		assert.Error(t, err)
		assert.Nil(t, db)
	})
}

type testError struct{}

func (e testError) Error() string {
	return "test-error"
}

var invalidKeys = []string{"", " a", "a ", ".", "..", "/", "a/b", "\\", "a\\b"}

func TestDB_Reader(t *testing.T) {
	t.Run("should return error for invalid keys", func(t *testing.T) {
		for _, key := range invalidKeys {
			t.Run(key, func(t *testing.T) {
				db := openDB(t, fake.ExistingDir())
				// when
				reader, err := db.Reader(key)
				// then
				assert.Nil(t, reader)
				assert.True(t, deebee.IsClientError(err))
			})
		}
	})

	t.Run("should return error when no data was previously saved", func(t *testing.T) {
		db := openDB(t, fake.ExistingDir())
		// when
		reader, err := db.Reader("state")
		// then
		assert.Nil(t, reader)
		assert.False(t, deebee.IsClientError(err))
		assert.True(t, deebee.IsDataNotFound(err))
	})

	t.Run("should return error when DB is failing", func(t *testing.T) {
		dirs := map[string]deebee.Dir{
			"ListFiles":  failing.ListFiles(fake.ExistingDir()),
			"FileReader": failing.FileReader(fake.ExistingDir()),
		}
		for name, dir := range dirs {

			t.Run(name, func(t *testing.T) {
				db := openDB(t, dir)
				if err := writeDataOrError(db, "key", []byte("data")); err != nil {
					return
				}
				// when
				reader, err := db.Reader("key")
				// then
				assert.Error(t, err)
				assert.Nil(t, reader)
			})
		}
	})
}

func TestDB_Writer(t *testing.T) {
	t.Run("should return error for invalid keys", func(t *testing.T) {
		for _, key := range invalidKeys {
			t.Run(key, func(t *testing.T) {
				db := openDB(t, fake.ExistingDir())
				// when
				writer, err := db.Writer(key)
				// then
				assert.Nil(t, writer)
				assert.True(t, deebee.IsClientError(err))
			})
		}
	})

	t.Run("should return error when DB is failing", func(t *testing.T) {
		dirs := map[string]deebee.Dir{
			"Mkdir":      failing.Mkdir(fake.ExistingDir()),
			"FileWriter": failing.FileWriter(fake.ExistingDir()),
		}
		for name, dir := range dirs {

			t.Run(name, func(t *testing.T) {
				db := openDB(t, dir)
				// when
				writer, err := db.Writer("key")
				// then
				assert.Error(t, err)
				assert.Nil(t, writer)
			})
		}
	})
}

func TestReadAfterWrite(t *testing.T) {
	const key = "state"

	t.Run("should read previously written data", func(t *testing.T) {
		tests := map[string][]byte{
			"empty":      {},
			"data":       []byte("data"),
			"MB of data": makeData(1024*1024, 1),
		}
		for name, data := range tests {

			t.Run(name, func(t *testing.T) {
				db := openDB(t, fake.ExistingDir())
				writeData(t, db, key, data)
				// when
				actual := readData(t, db, key)
				// then
				assert.Equal(t, data, actual)
			})
		}
	})

	t.Run("after update should read last written data", func(t *testing.T) {
		db := openDB(t, fake.ExistingDir())
		updatedData := "updated"
		writeData(t, db, key, []byte("data"))
		writeData(t, db, key, []byte(updatedData))
		// when
		actual := readData(t, db, key)
		// then
		assert.Equal(t, updatedData, string(actual))
	})

	t.Run("after two updates should read last written data", func(t *testing.T) {
		db := openDB(t, fake.ExistingDir())
		updatedData := "updated"
		writeData(t, db, key, []byte("data1"))
		writeData(t, db, key, []byte("data2"))
		writeData(t, db, key, []byte(updatedData))
		// when
		actual := readData(t, db, key)
		// then
		assert.Equal(t, updatedData, string(actual))
	})

	t.Run("should update data using different db instance", func(t *testing.T) {
		dir := fake.ExistingDir()
		db := openDB(t, dir)
		writeData(t, db, key, []byte("data"))

		anotherDb := openDB(t, dir)
		updatedData := "updated"
		writeData(t, anotherDb, key, []byte(updatedData))
		// when
		actual := readData(t, anotherDb, key)
		// then
		assert.Equal(t, updatedData, string(actual))
	})

	t.Run("should return data not found error when all files are corrupted", func(t *testing.T) {
		tests := map[string]int{
			"no updates": 0,
			"one update": 1,
		}
		for name, numberOfUpdates := range tests {

			t.Run(name, func(t *testing.T) {
				dir := fake.ExistingDir()
				db := openDB(t, dir)
				writeData(t, db, key, []byte("new"))

				for i := 0; i < numberOfUpdates; i++ {
					writeData(t, db, key, []byte("update"))
				}
				// when
				corruptAllFiles(dir, key)
				reader, err := db.Reader(key)
				// then
				require.Error(t, err)
				assert.True(t, deebee.IsDataNotFound(err))
				assert.Nil(t, reader)
			})
		}
	})

	t.Run("should return error when file was integral during verification, but became corrupted during read", func(t *testing.T) {
		dir := fake.ExistingDir()
		db := openDB(t, dir)
		writeData(t, db, key, []byte("data"))
		reader, err := db.Reader(key)
		require.NoError(t, err)
		corruptAllFiles(dir, key)
		_, err = ioutil.ReadAll(reader)
		require.NoError(t, err)
		// when
		err = reader.Close()
		// then
		assert.Error(t, err)
	})

	t.Run("should return last not corrupted data", func(t *testing.T) {
		dir := fake.ExistingDir()
		db := openDB(t, dir)
		writeData(t, db, key, []byte("old"))
		writeData(t, db, key, []byte("new"))
		// when
		corruptLastAddedFile(dir, key)
		reader, err := db.Reader(key)
		// then
		require.NoError(t, err)
		assert.NotNil(t, reader)
	})
}

func TestFileIntegrityChecker(t *testing.T) {
	t.Run("should use custom FileIntegrityChecker", func(t *testing.T) {
		dir := fake.ExistingDir()
		db, err := deebee.Open(dir, deebee.CustomIntegrityChecker(&nullIntegrityChecker{}))
		require.NoError(t, err)
		notExpected := []byte("data")
		writeData(t, db, "key", notExpected)
		// when
		corruptAllFiles(dir, "key")
		// then
		data := readData(t, db, "key")
		assert.NotEqual(t, notExpected, data)
	})
}

//  Does not check integrity at all
type nullIntegrityChecker struct{}

// Returns random file
func (c *nullIntegrityChecker) LatestIntegralFilename(dir deebee.Dir) (string, error) {
	files, err := dir.ListFiles()
	if err != nil {
		return "", err
	}
	return files[0], nil
}

func (c *nullIntegrityChecker) DecorateReader(reader io.ReadCloser, dir deebee.Dir, name string) io.ReadCloser {
	return reader
}

func (c *nullIntegrityChecker) DecorateWriter(writer io.WriteCloser, dir deebee.Dir, name string) io.WriteCloser {
	return writer
}

func openDB(t *testing.T, dir deebee.Dir) *deebee.DB {
	db, err := deebee.Open(dir)
	require.NoError(t, err)
	return db
}

func writeData(t *testing.T, db *deebee.DB, key string, data []byte) {
	writer, err := db.Writer(key)
	require.NoError(t, err)
	_, err = writer.Write(data)
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)
}

func writeDataOrError(db *deebee.DB, key string, data []byte) error {
	writer, err := db.Writer(key)
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

func readData(t *testing.T, db *deebee.DB, key string) []byte {
	reader, err := db.Reader(key)
	require.NoError(t, err)
	actual, err := ioutil.ReadAll(reader)
	require.NoError(t, err)
	err = reader.Close()
	require.NoError(t, err)
	return actual
}

func makeData(size int, fillWith byte) []byte {
	data := make([]byte, size)
	for i := 0; i < size; i++ {
		data[i] = fillWith
	}
	return data
}

func corruptAllFiles(dir fake.Dir, key string) {
	stateDir := dir.FakeDir(key)
	files := stateDir.Files()
	for _, file := range files {
		file.Corrupt()
	}
}

func corruptLastAddedFile(dir fake.Dir, key string) {
	stateDir := dir.FakeDir(key)
	files := stateDir.Files()
	files[len(files)-1].Corrupt()
}
