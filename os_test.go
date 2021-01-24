package deebee_test

import (
	"io/ioutil"
	"path/filepath"
	"testing"

	"github.com/jacekolszak/deebee"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var data = []byte("test")

// TODO Reuse tests from fake package - behaviour of all adapters should be defined in well written reusable tests
func TestOsDir_FileWriter(t *testing.T) {
	t.Run("should open new file", func(t *testing.T) {
		osDir := deebee.OsDir(createTempDir(t))
		// when
		writer, err := osDir.FileWriter("file")
		require.NoError(t, err)
		defer writer.Close()
		require.NoError(t, err)
		assert.NotNil(t, writer)
	})

	t.Run("should return error when file already exists", func(t *testing.T) {
		dir := createTempDir(t)
		writeFile(t, dir, "existing-file")
		osDir := deebee.OsDir(dir)
		// when
		writer, err := osDir.FileWriter("existing-file")
		require.Error(t, err)
		assert.Nil(t, writer)
	})

	t.Run("should open file for write", func(t *testing.T) {
		osDir := deebee.OsDir(createTempDir(t))
		// when
		writer, err := osDir.FileWriter("file")
		require.NoError(t, err)
		// then
		_, err = writer.Write(data)
		require.NoError(t, err)
		err = writer.Close()
		require.NoError(t, err)
	})
}

func TestOsDir_FileReader(t *testing.T) {
	t.Run("should return error when file does not exist", func(t *testing.T) {
		osDir := deebee.OsDir(createTempDir(t))
		// when
		reader, err := osDir.FileReader("not-existing-file")
		require.Error(t, err)
		assert.Nil(t, reader)
	})

	t.Run("should open existing file", func(t *testing.T) {
		dir := createTempDir(t)
		osDir := deebee.OsDir(dir)
		writeFile(t, dir, "existing-file")
		// when
		reader, err := osDir.FileReader("existing-file")
		require.NoError(t, err)
		defer reader.Close()
		// then
		require.NoError(t, err)
		assert.NotNil(t, reader)
	})

	t.Run("should read file", func(t *testing.T) {
		dir := createTempDir(t)
		osDir := deebee.OsDir(dir)
		writeFile(t, dir, "existing-file")
		// when
		reader, err := osDir.FileReader("existing-file")
		require.NoError(t, err)
		defer reader.Close()
		// then
		actualData, err := ioutil.ReadAll(reader)
		require.NoError(t, err)
		assert.Equal(t, actualData, data)
	})
}

func createTempDir(t *testing.T) string {
	dir, err := ioutil.TempDir("", "test")
	require.NoError(t, err)
	return dir
}

func writeFile(t *testing.T, dir, file string) {
	f := filepath.Join(dir, file)
	err := ioutil.WriteFile(f, data, 0666)
	require.NoError(t, err)
}
