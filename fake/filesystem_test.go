package fake_test

import (
	"testing"

	"github.com/jacekolszak/deebee"
	"github.com/jacekolszak/deebee/fake"
	"github.com/jacekolszak/deebee/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const fileName = "test"

var dirs = map[string]func(t *testing.T) deebee.Dir{
	"root":   rootDir,
	"nested": makeNestedDir,
}

func rootDir(t *testing.T) deebee.Dir {
	return &fake.Dir{}
}

func makeNestedDir(t *testing.T) deebee.Dir {
	dir := &fake.Dir{}
	err := dir.Mkdir("nested")
	require.NoError(t, err)
	return dir.Dir("nested")
}

func TestDir_FileWriter(t *testing.T) {
	test.TestDir_FileWriter(t, dirs)
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
	test.TestFileWriter_Write(t, dirs)
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
	test.TestDir_FileReader(t, dirs)
}

func TestFile_Read(t *testing.T) {
	test.TestFileReader_Read(t, dirs)
}

func TestDir_DirExists(t *testing.T) {
	test.TestDir_DirExists(t, dirs)
}

func TestDir_Mkdir(t *testing.T) {
	test.TestDir_Mkdir(t, dirs)
}

func TestDir_Dir(t *testing.T) {
	test.TestDir_Dir(t, dirs)
}

func TestDir_ListFiles(t *testing.T) {
	test.TestDir_ListFiles(t, dirs)
}
