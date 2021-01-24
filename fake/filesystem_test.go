package fake_test

import (
	"io/ioutil"
	"testing"

	"github.com/jacekolszak/deebee"
	"github.com/jacekolszak/deebee/fake"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const fileName = "test"

func dirs(t *testing.T) map[string]func(t *testing.T) deebee.Dir {
	return map[string]func(*testing.T) deebee.Dir{
		"root":   rootDir,
		"nested": makeNestedDir,
	}
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
	for dirType, newDir := range dirs(t) {
		t.Run(dirType, func(t *testing.T) {

			t.Run("should return error for empty name", func(t *testing.T) {
				file, err := newDir(t).FileWriter("")
				require.Error(t, err)
				assert.Nil(t, file)
			})

			t.Run("should return error when file already exists", func(t *testing.T) {
				dir := newDir(t)
				writeFile(t, dir, fileName, []byte{})
				// when
				file, err := dir.FileWriter(fileName)
				require.Error(t, err)
				assert.Nil(t, file)
			})
		})
	}
}

func writeFile(t *testing.T, dir deebee.Dir, name string, data []byte) {
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
	for dirType, newDir := range dirs(t) {
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
	for dirType, newDir := range dirs(t) {
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
				writeFile(t, dir, fileName, []byte{})
				// when
				file, err := dir.FileReader(fileName)
				// then
				require.NoError(t, err)
				assert.NotNil(t, file)
			})
		})
	}
}

func TestFile_Read(t *testing.T) {
	for dirType, newDir := range dirs(t) {
		t.Run(dirType, func(t *testing.T) {

			t.Run("should read previously written data", func(t *testing.T) {
				dir := newDir(t)
				data := []byte("payload")
				writeFile(t, dir, fileName, data)
				// when
				actual := readFile(t, dir, fileName)
				// then
				assert.Equal(t, data, actual)
			})

			t.Run("should read empty slice after EOF", func(t *testing.T) {
				dir := newDir(t)
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
				dir := newDir(t)
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
		})
	}
}

func TestDir_DirExists(t *testing.T) {
	for dirType, newDir := range dirs(t) {
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

func TestDir_Mkdir(t *testing.T) {
	for dirType, newDir := range dirs(t) {
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
				nested := mkdir(t, dir, "nested")
				writeFile(t, nested, "name", []byte{})
				// when
				nested = mkdir(t, dir, "nested")
				// then
				files, err := nested.ListFiles()
				require.NoError(t, err)
				assert.Equal(t, []string{"name"}, files)
			})
		})
	}
}

func mkdir(t *testing.T, dir deebee.Dir, name string) deebee.Dir {
	err := dir.Mkdir(name)
	require.NoError(t, err)
	return dir.Dir(name)
}

func TestDir_Dir(t *testing.T) {
	t.Run("should return nested dir", func(t *testing.T) {
		dir := &fake.Dir{}
		nested := dir.Dir("name")
		assert.NotNil(t, nested)
	})
}

func TestDir_ListFiles(t *testing.T) {
	for dirType, newDir := range dirs(t) {
		t.Run(dirType, func(t *testing.T) {

			t.Run("for empty dir returns empty slice", func(t *testing.T) {
				files, err := newDir(t).ListFiles()
				require.NoError(t, err)
				assert.Empty(t, files)
			})

			t.Run("should return two files", func(t *testing.T) {
				dir := newDir(t)
				writeFile(t, dir, "name1", []byte("Hello"))
				writeFile(t, dir, "name2", []byte("Hello"))
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
				writeFile(t, dir1, "name", data)
				// when
				dir2 := dir.Dir("nested")
				assert.Equal(t, data, readFile(t, dir2, "name"))
			})
		})
	}
}

func readFile(t *testing.T, dir deebee.Dir, name string) []byte {
	reader, err := dir.FileReader(name)
	require.NoError(t, err)
	data, err := ioutil.ReadAll(reader)
	require.NoError(t, err)
	return data
}
