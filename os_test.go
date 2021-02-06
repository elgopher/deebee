package deebee_test

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/jacekolszak/deebee"
	"github.com/jacekolszak/deebee/test"
	"github.com/stretchr/testify/require"
)

var dirs = map[string]test.NewDir{
	"existing root": existingRootDir,
	"created root":  makeRootDir,
	"nested":        makeNestedDir,
}

func existingRootDir(t *testing.T) deebee.Dir {
	return deebee.OsDir(createTempDir(t))
}

func makeRootDir(t *testing.T) deebee.Dir {
	dir := createTempDir(t)
	err := os.RemoveAll(dir)
	require.NoError(t, err)
	missing := deebee.OsDir(dir)
	err = missing.Mkdir()
	require.NoError(t, err)
	return missing
}

func makeNestedDir(t *testing.T) deebee.Dir {
	dir := existingRootDir(t)
	err := dir.Dir("nested").Mkdir()
	require.NoError(t, err)
	return dir.Dir("nested")
}

func TestOsDir_FileWriter(t *testing.T) {
	test.TestDir_FileWriter(t, dirs)
}

func TestFileWriter_Write(t *testing.T) {
	test.TestFileWriter_Write(t, dirs)
}

func TestOsDir_FileReader(t *testing.T) {
	test.TestDir_FileReader(t, dirs)
}

func TestFileReader_Read(t *testing.T) {
	test.TestFileReader_Read(t, dirs)
}

func TestOsDir_Exists(t *testing.T) {
	test.TestDir_Exists(t, dirs)
}

func TestOsDir_Mkdir(t *testing.T) {
	test.TestDir_Mkdir(t, dirs)
}

func TestOsDir_Dir(t *testing.T) {
	test.TestDir_Dir(t, dirs)
}

func TestOsDir_ListFiles(t *testing.T) {
	test.TestDir_ListFiles(t, dirs)
}

func TestOsDir_DeleteFile(t *testing.T) {
	test.TestDir_DeleteFile(t, dirs)
}

func createTempDir(t *testing.T) string {
	dir, err := ioutil.TempDir("", "test")
	require.NoError(t, err)
	return dir
}
