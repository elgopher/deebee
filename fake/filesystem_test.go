package fake_test

import (
	"io/ioutil"
	"testing"

	"github.com/jacekolszak/deebee/fake"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const fileName = "test"

func TestDir_FileWriter(t *testing.T) {
	t.Run("should return error for empty name", func(t *testing.T) {
		dir := &fake.Dir{}
		// when
		file, err := dir.FileWriter("")
		// then
		require.Error(t, err)
		assert.Nil(t, file)
	})

	t.Run("should return error when file already exists", func(t *testing.T) {
		dir := &fake.Dir{}
		writeFile(t, dir, fileName, []byte{})
		// when
		file, err := dir.FileWriter(fileName)
		// then
		require.Error(t, err)
		assert.Nil(t, file)
	})
}

func writeFile(t *testing.T, dir *fake.Dir, name string, data []byte) {
	file, err := dir.FileWriter(name)
	require.NoError(t, err)

	_, err = file.Write(data)
	require.NoError(t, err)

	err = file.Close()
	require.NoError(t, err)
}

func TestDir_Files(t *testing.T) {
	t.Run("by default should return empty slice", func(t *testing.T) {
		dir := &fake.Dir{}
		assert.Empty(t, dir.Files())
	})
}

func TestFile_Close(t *testing.T) {
	t.Run("should create empty file", func(t *testing.T) {
		dir := &fake.Dir{}
		file, _ := dir.FileWriter(fileName)
		// when
		err := file.Close()
		// then
		require.NoError(t, err)
		actualFiles := dir.Files()
		assert.Len(t, actualFiles, 1)
		actualFile := actualFiles[0]
		assert.True(t, actualFile.Empty())
		assert.True(t, actualFile.Closed())
	})

	t.Run("should write file", func(t *testing.T) {
		dir := &fake.Dir{}
		file, _ := dir.FileWriter(fileName)
		data := []byte("payload")
		_, err := file.Write(data)
		require.NoError(t, err)
		// when
		err = file.Close()
		// then
		require.NoError(t, err)
		actualFiles := dir.Files()
		assert.Len(t, actualFiles, 1)
		assert.Equal(t, data, actualFiles[0].Data())
	})
}

func TestFile_Write(t *testing.T) {
	t.Run("should return error when trying to write to closed file", func(t *testing.T) {
		dir := &fake.Dir{}
		file, err := dir.FileWriter(fileName)
		require.NoError(t, err)
		err = file.Close()
		require.NoError(t, err)
		// when
		n, err := file.Write([]byte("payload"))
		require.Error(t, err)
		assert.Equal(t, 0, n)
	})
}

func TestFile_Sync(t *testing.T) {
	t.Run("Sync should update SyncedData", func(t *testing.T) {
		var (
			dir     = &fake.Dir{}
			file, _ = dir.FileWriter(fileName)
			data    = []byte("payload")
			_, _    = file.Write(data)
		)
		// when
		err := file.Sync()
		// then
		require.NoError(t, err)
		require.Len(t, dir.Files(), 1)
		actualFile := dir.Files()[0]
		assert.Equal(t, data, actualFile.SyncedData())
	})
}

func TestFile_SyncedData(t *testing.T) {
	t.Run("should return empty for not synced file", func(t *testing.T) {
		var (
			dir     = &fake.Dir{}
			file, _ = dir.FileWriter(fileName)
			data    = []byte("payload")
			_, _    = file.Write(data)
		)
		require.Len(t, dir.Files(), 1)
		actualFile := dir.Files()[0]
		// expect
		assert.Empty(t, actualFile.SyncedData())
	})
}

func TestDir_FileReader(t *testing.T) {
	t.Run("should return error for empty name", func(t *testing.T) {
		dir := &fake.Dir{}
		// when
		file, err := dir.FileReader("")
		// then
		require.Error(t, err)
		assert.Nil(t, file)
	})

	t.Run("should return error when file does not exist", func(t *testing.T) {
		dir := &fake.Dir{}
		// when
		file, err := dir.FileReader(fileName)
		// then
		require.Error(t, err)
		assert.Nil(t, file)
	})

	t.Run("should open existing file", func(t *testing.T) {
		dir := &fake.Dir{}
		writeFile(t, dir, fileName, []byte{})
		// when
		file, err := dir.FileReader(fileName)
		// then
		require.NoError(t, err)
		assert.NotNil(t, file)
	})
}

func TestFile_Read(t *testing.T) {
	t.Run("should read previously written data", func(t *testing.T) {
		dir := &fake.Dir{}
		data := []byte("payload")
		writeFile(t, dir, fileName, data)
		file, err := dir.FileReader(fileName)
		require.NoError(t, err)
		// when
		actual, err := ioutil.ReadAll(file)
		// then
		require.NoError(t, err)
		assert.Equal(t, data, actual)
	})

	t.Run("should read empty slice after EOF", func(t *testing.T) {
		dir := &fake.Dir{}
		data := []byte("payload")
		writeFile(t, dir, fileName, data)
		file, err := dir.FileReader(fileName)
		require.NoError(t, err)
		_, _ = ioutil.ReadAll(file)
		// when
		actual, err := ioutil.ReadAll(file)
		// then
		require.NoError(t, err)
		assert.Empty(t, actual)
	})

	t.Run("should return error when trying to read from closed file", func(t *testing.T) {
		dir := &fake.Dir{}
		data := []byte("payload")
		writeFile(t, dir, fileName, data)
		file, err := dir.FileReader(fileName)
		require.NoError(t, err)
		err = file.Close()
		require.NoError(t, err)
		output := make([]byte, 1024)
		// when
		n, err := file.Read(output)
		// then
		require.Error(t, err)
		assert.Equal(t, 0, n)
	})
}
