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

func TestDB_Reader(t *testing.T) {
	t.Run("should return error when no data was previously saved", func(t *testing.T) {
		db := openDB(t, fake.ExistingDir())
		// when
		reader, err := db.Reader()
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
				if err := writeDataOrError(db, []byte("data")); err != nil {
					return
				}
				// when
				reader, err := db.Reader()
				// then
				assert.Error(t, err)
				assert.Nil(t, reader)
			})
		}
	})
}

func TestDB_Writer(t *testing.T) {
	t.Run("should return error when DB is failing", func(t *testing.T) {
		dirs := map[string]deebee.Dir{
			"FileWriter": failing.FileWriter(fake.ExistingDir()),
		}
		for name, dir := range dirs {

			t.Run(name, func(t *testing.T) {
				db := openDB(t, dir)
				// when
				writer, err := db.Writer()
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
				db := openDB(t, fake.ExistingDir())
				writeData(t, db, data)
				// when
				actual := readData(t, db)
				// then
				assert.Equal(t, data, actual)
			})
		}
	})

	t.Run("after update should read last written data", func(t *testing.T) {
		db := openDB(t, fake.ExistingDir())
		updatedData := "updated"
		writeData(t, db, []byte("data"))
		writeData(t, db, []byte(updatedData))
		// when
		actual := readData(t, db)
		// then
		assert.Equal(t, updatedData, string(actual))
	})

	t.Run("after two updates should read last written data", func(t *testing.T) {
		db := openDB(t, fake.ExistingDir())
		updatedData := "updated"
		writeData(t, db, []byte("data1"))
		writeData(t, db, []byte("data2"))
		writeData(t, db, []byte(updatedData))
		// when
		actual := readData(t, db)
		// then
		assert.Equal(t, updatedData, string(actual))
	})

	t.Run("should update data using different db instance", func(t *testing.T) {
		dir := fake.ExistingDir()
		db := openDB(t, dir)
		writeData(t, db, []byte("data"))

		anotherDb := openDB(t, dir)
		updatedData := "updated"
		writeData(t, anotherDb, []byte(updatedData))
		// when
		actual := readData(t, anotherDb)
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
				writeData(t, db, []byte("new"))

				for i := 0; i < numberOfUpdates; i++ {
					writeData(t, db, []byte("update"))
				}
				// when
				corruptAllFiles(dir)
				reader, err := db.Reader()
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
		writeData(t, db, []byte("data"))
		reader, err := db.Reader()
		require.NoError(t, err)
		corruptAllFiles(dir)
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
		writeData(t, db, []byte("old"))
		writeData(t, db, []byte("new"))
		// when
		corruptLastAddedFile(dir)
		reader, err := db.Reader()
		// then
		require.NoError(t, err)
		assert.NotNil(t, reader)
	})
}

func TestIntegrityChecker(t *testing.T) {
	t.Run("should use custom DataIntegrityChecker", func(t *testing.T) {
		dir := fake.ExistingDir()
		db, err := deebee.Open(dir, deebee.IntegrityChecker(&nullIntegrityChecker{}))
		require.NoError(t, err)
		notExpected := []byte("data")
		writeData(t, db, notExpected)
		// when
		corruptAllFiles(dir)
		// then
		data := readData(t, db)
		assert.NotEqual(t, notExpected, data)
	})
}

//  Does not check integrity at all
type nullIntegrityChecker struct{}

func (c *nullIntegrityChecker) DecorateReader(reader io.ReadCloser, name string, readChecksum deebee.ReadChecksum) io.ReadCloser {
	return reader
}

func (c *nullIntegrityChecker) DecorateWriter(writer io.WriteCloser, name string, writeChecksum deebee.WriteChecksum) io.WriteCloser {
	return writer
}

func openDB(t *testing.T, dir deebee.Dir) *deebee.DB {
	db, err := deebee.Open(dir)
	require.NoError(t, err)
	return db
}

func writeData(t *testing.T, db *deebee.DB, data []byte) {
	writer, err := db.Writer()
	require.NoError(t, err)
	_, err = writer.Write(data)
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)
}

func writeDataOrError(db *deebee.DB, data []byte) error {
	writer, err := db.Writer()
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

func readData(t *testing.T, db *deebee.DB) []byte {
	reader, err := db.Reader()
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

func corruptAllFiles(dir fake.Dir) {
	files := dir.Files()
	for _, file := range files {
		file.Corrupt()
	}
}

func corruptLastAddedFile(dir fake.Dir) {
	files := dir.Files()
	files[len(files)-1].Corrupt()
}
