// (c) 2021 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

package store_test

import (
	"errors"
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/jacekolszak/deebee/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpen(t *testing.T) {

	t.Run("should return error when dir is empty", func(t *testing.T) {
		s, err := store.Open("")
		require.Error(t, err)
		assert.Nil(t, s)
	})

	t.Run("should return error when dir is not a directory", func(t *testing.T) {
		invalidDir := path.Join(tempDir(t), "file")
		touchFile(t, invalidDir)
		// when
		s, err := store.Open(invalidDir)
		// then
		require.Error(t, err)
		assert.Nil(t, s)
	})

	t.Run("by default should create dir if does not exist", func(t *testing.T) {
		dir := path.Join(tempDir(t), "missing")
		_, err := store.Open(dir)
		require.NoError(t, err)
		assert.DirExists(t, dir)
	})

	t.Run("by default should create nested dir if does not exist", func(t *testing.T) {
		dir := path.Join(tempDir(t), "nested", "missing")
		_, err := store.Open(dir)
		require.NoError(t, err)
		assert.DirExists(t, dir)
	})

	t.Run("should return error when dir does not exist and FailWhenMissingDir option was used", func(t *testing.T) {
		dir := path.Join(tempDir(t), "missing")
		s, err := store.Open(dir, store.FailWhenMissingDir)
		require.Error(t, err)
		assert.Nil(t, s)
	})

	t.Run("should accept nil option", func(t *testing.T) {
		s, err := store.Open(tempDir(t), nil)
		require.NoError(t, err)
		assert.NotNil(t, s)
	})

	t.Run("should return error when option returned error", func(t *testing.T) {
		option := func(*store.Store) error {
			return errors.New("error")
		}
		s, err := store.Open(tempDir(t), option)
		assert.Error(t, err)
		assert.Nil(t, s)
	})
}

func TestReadAfterWrite(t *testing.T) {

	t.Run("should read previously written data", func(t *testing.T) {
		s := openStore(t)
		data := []byte("data")
		writeData(t, s, data)
		dataRead := readData(t, s)
		assert.Equal(t, data, dataRead)
	})

	t.Run("should read updated data", func(t *testing.T) {
		s := openStore(t)
		newData := []byte("new")
		writeData(t, s, []byte("old data"))
		writeData(t, s, newData)
		dataRead := readData(t, s)
		assert.Equal(t, newData, dataRead)
	})
}

func tempDir(t *testing.T) string {
	dir, err := ioutil.TempDir("", "deebee")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, os.RemoveAll(dir))
	})
	return dir
}

func touchFile(t *testing.T, path string) {
	err := ioutil.WriteFile(path, []byte{}, 0664)
	require.NoError(t, err)
}
