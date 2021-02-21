// (c) 2021 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

package os_test

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/jacekolszak/deebee/dirtest"
	standardos "github.com/jacekolszak/deebee/os"
	"github.com/jacekolszak/deebee/store"
	"github.com/stretchr/testify/require"
)

var dirs = map[string]dirtest.NewDir{
	"existing root": existingRootDir,
	"created root":  makeRootDir,
	"nested":        makeNestedDir,
}

func existingRootDir(t *testing.T) store.Dir {
	return standardos.Dir(createTempDir(t))
}

func makeRootDir(t *testing.T) store.Dir {
	dir := createTempDir(t)
	err := os.RemoveAll(dir)
	require.NoError(t, err)
	missing := standardos.Dir(dir)
	err = missing.Mkdir()
	require.NoError(t, err)
	return missing
}

func makeNestedDir(t *testing.T) store.Dir {
	dir := existingRootDir(t)
	err := dir.Dir("nested").Mkdir()
	require.NoError(t, err)
	return dir.Dir("nested")
}

func TestOsDir_FileWriter(t *testing.T) {
	dirtest.TestDir_FileWriter(t, dirs)
}

func TestFileWriter_Write(t *testing.T) {
	dirtest.TestFileWriter_Write(t, dirs)
}

func TestOsDir_FileReader(t *testing.T) {
	dirtest.TestDir_FileReader(t, dirs)
}

func TestFileReader_Read(t *testing.T) {
	dirtest.TestFileReader_Read(t, dirs)
}

func TestOsDir_Exists(t *testing.T) {
	dirtest.TestDir_Exists(t, dirs)
}

func TestOsDir_Mkdir(t *testing.T) {
	dirtest.TestDir_Mkdir(t, dirs)
}

func TestOsDir_Dir(t *testing.T) {
	dirtest.TestDir_Dir(t, dirs)
}

func TestOsDir_ListFiles(t *testing.T) {
	dirtest.TestDir_ListFiles(t, dirs)
}

func TestOsDir_DeleteFile(t *testing.T) {
	dirtest.TestDir_DeleteFile(t, dirs)
}

func TestOsWriter_Sync(t *testing.T) {
	dirtest.TestFileWriter_Sync(t, dirs)
}

func TestDir_ThreadSafety(t *testing.T) {
	dirtest.TestDir_ThreadSafety(t, dirs)
}

func createTempDir(t *testing.T) string {
	dir, err := ioutil.TempDir("", "test")
	require.NoError(t, err)
	return dir
}
