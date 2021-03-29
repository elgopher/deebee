// (c) 2021 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

package tests

import (
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/jacekolszak/deebee/store"
	"github.com/stretchr/testify/require"
)

func OpenStore(t *testing.T, options ...store.Option) *store.Store {
	s, err := store.Open(TempDir(t), options...)
	require.NoError(t, err)
	return s
}

func TempDir(t *testing.T) string {
	dir, err := ioutil.TempDir("", "deebee")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, os.RemoveAll(dir))
	})
	return dir
}

func ReadData(t *testing.T, s *store.Store, options ...store.ReaderOption) []byte {
	reader, err := s.Reader(options...)
	require.NoError(t, err)
	bytes, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	return bytes
}

func WriteData(t *testing.T, s *store.Store, bytes []byte, writerOptions ...store.WriterOption) store.Version {
	writer, err := s.Writer(writerOptions...)
	require.NoError(t, err)
	_, err = writer.Write(bytes)
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)
	return writer.Version()
}
