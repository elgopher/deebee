// (c) 2021 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

package fake_test

import (
	"io/ioutil"
	"testing"

	"github.com/jacekolszak/deebee/dirtest"
	"github.com/jacekolszak/deebee/fake"
	"github.com/jacekolszak/deebee/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const fileName = "test"

var dirs = map[string]dirtest.NewDir{
	"existing root": existingRootDir,
	"created root":  makeRootDir,
	"nested":        makeNestedDir,
	"fakeDir":       fakeDir,
}

func existingRootDir(t *testing.T) store.Dir {
	return fake.ExistingDir()
}

func makeRootDir(t *testing.T) store.Dir {
	dir := fake.MissingDir()
	err := dir.Mkdir()
	require.NoError(t, err)
	return dir
}

func makeNestedDir(t *testing.T) store.Dir {
	dir := fake.ExistingDir()
	err := dir.Dir("nested").Mkdir()
	require.NoError(t, err)
	return dir.Dir("nested")
}

func fakeDir(t *testing.T) store.Dir {
	dir := fake.ExistingDir()
	err := dir.Dir("nested").Mkdir()
	require.NoError(t, err)
	return dir.FakeDir("nested")
}

func TestDir_FileWriter(t *testing.T) {
	dirtest.TestDir_FileWriter(t, dirs)
}

func TestDir_Files(t *testing.T) {
	t.Run("by default should return empty slice", func(t *testing.T) {
		dir := fake.ExistingDir()
		assert.Empty(t, dir.Files())
	})
}

func TestFile_Close(t *testing.T) {
	t.Run("should create empty file", func(t *testing.T) {
		dir := fake.ExistingDir()
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
		dir := fake.ExistingDir()
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
		actualFile := actualFiles[0]
		assert.Equal(t, data, actualFile.Data())
		assert.Equal(t, fileName, actualFile.Name())
	})
}

func TestFile_Write(t *testing.T) {
	dirtest.TestFileWriter_Write(t, dirs)
}

func TestFile_Sync(t *testing.T) {
	t.Run("Sync should update SyncedData", func(t *testing.T) {
		var (
			dir     = fake.ExistingDir()
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

	dirtest.TestFileWriter_Sync(t, dirs)
}

func TestFile_SyncedData(t *testing.T) {
	t.Run("should return empty for not synced file", func(t *testing.T) {
		var (
			dir     = fake.ExistingDir()
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
	dirtest.TestDir_FileReader(t, dirs)
}

func TestFile_Read(t *testing.T) {
	dirtest.TestFileReader_Read(t, dirs)
}

func TestDir_Exists(t *testing.T) {
	dirtest.TestDir_Exists(t, dirs)
}

func TestDir_Mkdir(t *testing.T) {
	dirtest.TestDir_Mkdir(t, dirs)
}

func TestDir_Dir(t *testing.T) {
	dirtest.TestDir_Dir(t, dirs)
}

func TestDir_ListFiles(t *testing.T) {
	dirtest.TestDir_ListFiles(t, dirs)
}

func TestFile_Corrupt(t *testing.T) {
	t.Run("should corrupt file", func(t *testing.T) {
		dir := fake.ExistingDir()
		data := []byte("data")
		dirtest.WriteFile(t, dir, "file", data)
		file := dir.Files()[0]
		// when
		file.Corrupt()
		// then
		actual := dirtest.ReadFile(t, dir, "file")
		assert.NotEqual(t, data, actual)
	})

	t.Run("should corrupt already open file", func(t *testing.T) {
		dir := fake.ExistingDir()
		data := []byte("data")
		dirtest.WriteFile(t, dir, "file", data)
		reader, err := dir.FileReader("file")
		require.NoError(t, err)
		file := dir.Files()[0]
		// when
		file.Corrupt()
		// then
		actual, err := ioutil.ReadAll(reader)
		require.NoError(t, err)
		assert.NotEqual(t, data, actual)
	})
}

func TestDir_FakeDir(t *testing.T) {
	t.Run("should return FakeDir with additional extending Dir", func(t *testing.T) {
		dir := fake.ExistingDir()
		dirtest.Mkdir(t, dir, "dir")
		// when
		fakeDir := dir.FakeDir("dir")
		assert.NotNil(t, fakeDir)
	})
}

func TestDir_DeleteFile(t *testing.T) {
	dirtest.TestDir_DeleteFile(t, dirs)
}

func TestDir_ThreadSafety(t *testing.T) {
	dirtest.TestDir_ThreadSafety(t, dirs,
		func(d store.Dir) { d.(fake.Dir).Files() },
		func(d store.Dir) { d.(fake.Dir).FakeDir("f") },
	)
}
