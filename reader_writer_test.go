package deebee_test

import (
	"io"
	"io/ioutil"
	"testing"

	"github.com/jacekolszak/deebee"
	"github.com/jacekolszak/deebee/fake"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadAfterWrite(t *testing.T) {
	t.Run("should read previously written data", func(t *testing.T) {
		data := []byte("data")
		dir := &fake.Dir{}
		writeData(t, dir, data)
		reader := openReader(t, dir)
		// when
		actual, err := ioutil.ReadAll(reader)
		// then
		require.NoError(t, err)
		assert.Equal(t, data, actual)
	})
}

func writeData(t *testing.T, dir *fake.Dir, data []byte) {
	writer := openWriter(t, dir)
	_, err := writer.Write(data)
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)
}

func openWriter(t *testing.T, dir *fake.Dir) io.WriteCloser {
	checksum := &fake.Checksum{}
	openWriter, err := deebee.OpenWriterFunc(checksum)
	require.NoError(t, err)
	writer, err := openWriter(dir)
	require.NoError(t, err)
	require.NotNil(t, writer)
	return writer
}

func openReader(t *testing.T, dir *fake.Dir) io.ReadCloser {
	checksum := &fake.Checksum{}
	openReader, err := deebee.OpenReaderFunc(checksum)
	require.NoError(t, err)
	writer, err := openReader(dir)
	require.NoError(t, err)
	require.NotNil(t, writer)
	return writer
}
