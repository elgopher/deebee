// (c) 2021 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

package storetest

import (
	"io/ioutil"
	"testing"

	"github.com/jacekolszak/deebee/store"
	"github.com/stretchr/testify/require"
)

func ReadData(t *testing.T, s *store.Store) []byte {
	reader, err := s.Reader()
	require.NoError(t, err)
	actual, err := ioutil.ReadAll(reader)
	require.NoError(t, err)
	err = reader.Close()
	require.NoError(t, err)
	return actual
}

func WriteData(t *testing.T, s *store.Store, data []byte) {
	writer, err := s.Writer()
	require.NoError(t, err)
	_, err = writer.Write(data)
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)
}
