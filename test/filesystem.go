// Package test provides reusable tests useful for testing new deebee.Dir implementations
package test

import (
	"io/ioutil"
	"testing"

	"github.com/jacekolszak/deebee"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const fileName = "test"

type NewDir func(t *testing.T) deebee.Dir

type Dirs map[string]NewDir

func WriteFile(t *testing.T, dir deebee.Dir, name string, data []byte) {
	file, err := dir.FileWriter(name)
	require.NoError(t, err)

	_, err = file.Write(data)
	require.NoError(t, err)

	err = file.Close()
	require.NoError(t, err)
}

func ReadFile(t *testing.T, dir deebee.Dir, name string) []byte {
	reader, err := dir.FileReader(name)
	require.NoError(t, err)
	data, err := ioutil.ReadAll(reader)
	require.NoError(t, err)
	return data
}

func Mkdir(t *testing.T, dir deebee.Dir, name string) deebee.Dir {
	err := dir.Mkdir(name)
	require.NoError(t, err)
	return dir.Dir(name)
}

func TestDir_FileWriter(t *testing.T, dirs Dirs) {
	for dirType, newDir := range dirs {
		t.Run(dirType, func(t *testing.T) {

			t.Run("should return error for empty name", func(t *testing.T) {
				file, err := newDir(t).FileWriter("")
				require.Error(t, err)
				assert.Nil(t, file)
			})

			t.Run("should return error when file already exists", func(t *testing.T) {
				dir := newDir(t)
				WriteFile(t, dir, fileName, []byte{})
				// when
				file, err := dir.FileWriter(fileName)
				require.Error(t, err)
				assert.Nil(t, file)
			})
		})
	}
}

func TestFileWriter_Write(t *testing.T, dirs Dirs) {
	for dirType, newDir := range dirs {
		t.Run(dirType, func(t *testing.T) {

			t.Run("should return error when trying to write to closed file", func(t *testing.T) {
				file, err := newDir(t).FileWriter(fileName)
				require.NoError(t, err)
				err = file.Close()
				require.NoError(t, err)
				// when
				n, err := file.Write([]byte("payload"))
				require.Error(t, err)
				assert.Equal(t, 0, n)
			})

		})
	}
}

func TestDir_FileReader(t *testing.T, dirs Dirs) {
	for dirType, newDir := range dirs {
		t.Run(dirType, func(t *testing.T) {

			t.Run("should return error for empty name", func(t *testing.T) {
				file, err := newDir(t).FileReader("")
				require.Error(t, err)
				assert.Nil(t, file)
			})

			t.Run("should return error when file does not exist", func(t *testing.T) {
				file, err := newDir(t).FileReader(fileName)
				require.Error(t, err)
				assert.Nil(t, file)
			})

			t.Run("should open existing file", func(t *testing.T) {
				dir := newDir(t)
				WriteFile(t, dir, fileName, []byte{})
				// when
				file, err := dir.FileReader(fileName)
				// then
				require.NoError(t, err)
				assert.NotNil(t, file)
			})
		})
	}
}

func TestFileReader_Read(t *testing.T, dirs Dirs) {
	for dirType, newDir := range dirs {
		t.Run(dirType, func(t *testing.T) {

			t.Run("should read previously written data", func(t *testing.T) {
				dir := newDir(t)
				data := []byte("payload")
				WriteFile(t, dir, fileName, data)
				// when
				actual := ReadFile(t, dir, fileName)
				// then
				assert.Equal(t, data, actual)
			})

			t.Run("should read empty slice after EOF", func(t *testing.T) {
				dir := newDir(t)
				data := []byte("payload")
				WriteFile(t, dir, fileName, data)
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
				dir := newDir(t)
				data := []byte("payload")
				WriteFile(t, dir, fileName, data)
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
		})
	}
}

func TestDir_DirExists(t *testing.T, dirs Dirs) {
	for dirType, newDir := range dirs {
		t.Run(dirType, func(t *testing.T) {

			t.Run("should return false for not existing dir", func(t *testing.T) {
				dir := newDir(t)
				exists, err := dir.DirExists("not-existing")
				require.NoError(t, err)
				assert.False(t, exists)
			})

			t.Run("should return true for previously created dir", func(t *testing.T) {
				dir := newDir(t)
				err := dir.Mkdir("existing")
				require.NoError(t, err)
				// when
				exists, err := dir.DirExists("existing")
				require.NoError(t, err)
				assert.True(t, exists)
			})

			t.Run("should return false when parent does not exist", func(t *testing.T) {
				dir := newDir(t)
				notExisting := dir.Dir("not-existing")
				// when
				exists, err := notExisting.DirExists("another")
				require.NoError(t, err)
				assert.False(t, exists)
			})
		})
	}
}

func TestDir_Mkdir(t *testing.T, dirs Dirs) {
	for dirType, newDir := range dirs {
		t.Run(dirType, func(t *testing.T) {

			t.Run("should allow creating dir twice", func(t *testing.T) {
				dir := newDir(t)
				err := dir.Mkdir("name")
				require.NoError(t, err)
				err = dir.Mkdir("name")
				require.NoError(t, err)
			})

			t.Run("should return error when dir does not exists", func(t *testing.T) {
				dir := newDir(t)
				notExistingDir := dir.Dir("not-existing-dir")
				// when
				err := notExistingDir.Mkdir("another")
				// then
				require.Error(t, err)
			})

			t.Run("creating dir twice does not override previously created files", func(t *testing.T) {
				dir := newDir(t)
				nested := Mkdir(t, dir, "nested")
				WriteFile(t, nested, "name", []byte{})
				// when
				nested = Mkdir(t, dir, "nested")
				// then
				files, err := nested.ListFiles()
				require.NoError(t, err)
				assert.Equal(t, []string{"name"}, files)
			})
		})
	}
}

func TestDir_Dir(t *testing.T, dirs Dirs) {
	for dirType, newDir := range dirs {
		t.Run(dirType, func(t *testing.T) {

			t.Run("should return nested dir", func(t *testing.T) {
				dir := newDir(t)
				nested := dir.Dir("name")
				assert.NotNil(t, nested)
			})
		})
	}
}

func TestDir_ListFiles(t *testing.T, dirs Dirs) {
	for dirType, newDir := range dirs {
		t.Run(dirType, func(t *testing.T) {

			t.Run("for empty dir returns empty slice", func(t *testing.T) {
				files, err := newDir(t).ListFiles()
				require.NoError(t, err)
				assert.Empty(t, files)
			})

			t.Run("should return two files", func(t *testing.T) {
				dir := newDir(t)
				WriteFile(t, dir, "name1", []byte("Hello"))
				WriteFile(t, dir, "name2", []byte("Hello"))
				// when
				files, err := dir.ListFiles()
				// then
				require.NoError(t, err)
				assert.Len(t, files, 2)
				assert.Contains(t, files, "name1")
				assert.Contains(t, files, "name2")
			})

			t.Run("should return error when dir does not exists", func(t *testing.T) {
				dir := newDir(t)
				notExistingDir := dir.Dir("not-existing-dir")
				// when
				files, err := notExistingDir.ListFiles()
				// then
				require.Error(t, err)
				assert.Nil(t, files)
			})

			t.Run("should return files only", func(t *testing.T) {
				dir := newDir(t)
				err := dir.Mkdir("excludedDir")
				require.NoError(t, err)
				// when
				files, err := dir.ListFiles()
				// then
				require.NoError(t, err)
				assert.Empty(t, files)
			})

			t.Run("should write and read file using different Dir instances", func(t *testing.T) {
				dir := newDir(t)
				err := dir.Mkdir("nested")
				require.NoError(t, err)
				dir1 := dir.Dir("nested")
				data := []byte("Hello")
				WriteFile(t, dir1, "name", data)
				// when
				dir2 := dir.Dir("nested")
				assert.Equal(t, data, ReadFile(t, dir2, "name"))
			})
		})
	}
}
