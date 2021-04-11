package tests

import (
	"io/ioutil"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func UpdateFiles(t *testing.T, dir, extension string, newContent string) {
	files, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, e := range files {
		if !e.IsDir() && strings.HasSuffix(e.Name(), extension) {
			err := ioutil.WriteFile(path.Join(dir, e.Name()), []byte(newContent), 0644)
			require.NoError(t, err)
		}
	}
}

func TouchFile(t *testing.T, path string) {
	err := ioutil.WriteFile(path, []byte{}, 0664)
	require.NoError(t, err)
}
