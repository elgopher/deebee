package tests

import (
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
)

func CorruptFiles(t *testing.T, dir string) {
	files, err := ioutil.ReadDir(dir)
	require.NoError(t, err)
	for _, file := range files {
		CorruptFile(t, path.Join(dir, file.Name()))
	}
}

func CorruptFile(t *testing.T, file string) {
	stat, err := os.Lstat(file)
	require.NoError(t, err)
	fileSize := stat.Size()

	f, err := os.OpenFile(file, os.O_RDWR, 0664)
	require.NoError(t, err)
	defer func() {
		_ = f.Close()
	}()

	var i int64
	for i = 0; i < fileSize; i += 1010 {
		corruptSingleByteAt(t, f, i)
	}
}

func corruptSingleByteAt(t *testing.T, f *os.File, offset int64) {
	b := make([]byte, 1)
	_, err := f.ReadAt(b, offset)
	require.NoError(t, err)
	b[0] = b[0] + 1
	_, err = f.WriteAt(b, offset)
	require.NoError(t, err)
}
