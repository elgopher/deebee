package deebee_test

import (
	"testing"

	"github.com/jacekolszak/deebee"
	"github.com/jacekolszak/deebee/fake"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpenReaderFunc(t *testing.T) {
	t.Run("should return error when checksum is nil", func(t *testing.T) {
		openReader, err := deebee.OpenReaderFunc(nil)
		require.Error(t, err)
		assert.Nil(t, openReader)
	})

	t.Run("should return OpenWriter function", func(t *testing.T) {
		checksum := &fake.Checksum{}
		openReader, err := deebee.OpenReaderFunc(checksum)
		require.NoError(t, err)
		assert.NotNil(t, openReader)
	})
}

func TestOpenReader(t *testing.T) {
	t.Run("should return error when dir is nil", func(t *testing.T) {
		checksum := &fake.Checksum{}
		openReader, _ := deebee.OpenReaderFunc(checksum)
		// when
		writer, err := openReader(nil)
		require.Error(t, err)
		assert.Nil(t, writer)
	})

	t.Run("should return error when no data was previously saved", func(t *testing.T) {
		checksum := &fake.Checksum{}
		openReader, _ := deebee.OpenReaderFunc(checksum)
		dir := &fake.Dir{}
		// when
		writer, err := openReader(dir)
		require.Error(t, err)
		assert.Nil(t, writer)
	})
}
