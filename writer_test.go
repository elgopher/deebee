package deebee_test

import (
	"testing"

	"github.com/jacekolszak/deebee"
	"github.com/jacekolszak/deebee/fake"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpenWriterFunc(t *testing.T) {
	t.Run("should return error when checksum is nil", func(t *testing.T) {
		openWriter, err := deebee.OpenWriterFunc(nil)
		require.Error(t, err)
		assert.Nil(t, openWriter)
	})

	t.Run("should return OpenWriter function", func(t *testing.T) {
		checksum := &fake.Checksum{}
		openWriter, err := deebee.OpenWriterFunc(checksum)
		require.NoError(t, err)
		assert.NotNil(t, openWriter)
	})
}

func TestOpenWriter(t *testing.T) {
	t.Run("should return error when dir is nil", func(t *testing.T) {
		checksum := &fake.Checksum{}
		openWriter, _ := deebee.OpenWriterFunc(checksum)
		// when
		writer, err := openWriter(nil)
		require.Error(t, err)
		assert.Nil(t, writer)
	})

	t.Run("should return writer", func(t *testing.T) {
		checksum := &fake.Checksum{}
		openWriter, _ := deebee.OpenWriterFunc(checksum)
		dir := &fake.Dir{}
		// when
		writer, err := openWriter(dir)
		require.NoError(t, err)
		assert.NotNil(t, writer)
	})
}
