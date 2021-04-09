// (c) 2021 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

package codec_test

import (
	"errors"
	"io"
	"testing"

	"github.com/jacekolszak/deebee/codec"
	"github.com/jacekolszak/deebee/internal/tests"
	"github.com/jacekolszak/deebee/store"
	otiai10 "github.com/otiai10/copy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWrite(t *testing.T) {
	t.Run("should return error when no encoder is given", func(t *testing.T) {
		s := tests.OpenStore(t)
		err := codec.Write(s, nil)
		assert.Error(t, err)
	})

	t.Run("should write using encoder", func(t *testing.T) {
		s := tests.OpenStore(t)
		input := []byte("data")
		encoder := func(w io.Writer) error {
			_, e := w.Write(input)
			return e
		}
		// when
		err := codec.Write(s, encoder)
		// then
		require.NoError(t, err)
		output := tests.ReadData(t, s)
		assert.Equal(t, input, output)
	})

	t.Run("should abort writing on encoding error", func(t *testing.T) {
		s := tests.OpenStore(t)
		encoder := func(io.Writer) error {
			return errors.New("failed")
		}
		// when
		err := codec.Write(s, encoder)
		// then
		assert.Error(t, err)
		// and
		_, err = s.Reader()
		assert.True(t, store.IsVersionNotFound(err))
	})
}

func TestRead(t *testing.T) {
	t.Run("should return error when no decoder is given", func(t *testing.T) {
		s := tests.OpenStore(t)
		_, err := codec.Read(s, nil)
		assert.Error(t, err)
	})

	t.Run("should read using decoder", func(t *testing.T) {
		s := tests.OpenStore(t)
		input := []byte("input")
		v := tests.WriteData(t, s, input)
		decoder := &tests.FakeDecoder{}
		// when
		actualVersion, err := codec.Read(s, decoder.Decode)
		// then
		require.NoError(t, err)
		assert.Equal(t, input, decoder.DataRead())
		assert.True(t, v.Time.Equal(actualVersion.Time))
	})

	t.Run("should return error on unmarshalling error", func(t *testing.T) {
		s := tests.OpenStore(t)
		// when
		_, err := codec.Read(s, failingDecoder)
		// then
		assert.Error(t, err)
	})
}

func TestReadLatest(t *testing.T) {
	t.Run("should return error", func(t *testing.T) {
		t.Run("when no decoder is given", func(t *testing.T) {
			s := tests.OpenStore(t)
			_, err := codec.ReadLatest(s, nil)
			assert.Error(t, err)
		})

		t.Run("when no store is given", func(t *testing.T) {
			decoder := func(reader io.Reader) error {
				return nil
			}
			_, err := codec.ReadLatest(nil, decoder)
			assert.Error(t, err)
		})

		t.Run("when store is empty", func(t *testing.T) {
			s := &tests.StoreMock{
				ReturnVersions: nil,
			}
			f := &tests.FakeDecoder{}
			_, err := codec.ReadLatest(s, f.Decode)
			assert.True(t, store.IsVersionNotFound(err))
		})

		t.Run("when Store.Versions returns error", func(t *testing.T) {
			s := &tests.StoreMock{
				ReturnVersionsError: errors.New("listing versions failed"),
			}
			f := &tests.FakeDecoder{}
			_, err := codec.ReadLatest(s, f.Decode)
			assert.True(t, store.IsVersionNotFound(err))
		})

		t.Run("when Store.Reader returns error", func(t *testing.T) {
			s := &tests.StoreMock{
				ReturnVersions:    []store.Version{{}},
				ReturnReaderError: errors.New("opening reader failed"),
			}
			f := &tests.FakeDecoder{}
			_, err := codec.ReadLatest(s, f.Decode)
			assert.True(t, store.IsVersionNotFound(err))
		})

		t.Run("when decoder returned error", func(t *testing.T) {
			s := &tests.StoreMock{
				ReturnVersions: []store.Version{{}},
				ReturnReader:   &tests.ReaderMock{},
			}
			_, err := codec.ReadLatest(s, failingDecoder)
			assert.True(t, store.IsVersionNotFound(err))
		})

		t.Run("when decoder returned error for two versions", func(t *testing.T) {
			s := &tests.StoreMock{
				ReturnVersions: []store.Version{{}, {}},
				ReturnReader:   &tests.ReaderMock{},
			}
			_, err := codec.ReadLatest(s, failingDecoder)
			assert.True(t, store.IsVersionNotFound(err))
		})
	})

	t.Run("should return a single version available", func(t *testing.T) {
		s := tests.OpenStore(t)
		data := []byte("data")
		version := tests.WriteData(t, s, data)
		f := &tests.FakeDecoder{}
		// when
		actualVersion, err := codec.ReadLatest(s, f.Decode)
		// then
		assert.NoError(t, err)
		assert.True(t, version.Time.Equal(actualVersion.Time))
		assert.Equal(t, data, f.DataRead())
	})

	t.Run("should read latest version", func(t *testing.T) {
		s := tests.OpenStore(t)
		tests.WriteData(t, s, []byte("first"))
		second := []byte("second")
		secondVersion := tests.WriteData(t, s, second)
		f := &tests.FakeDecoder{}
		// when
		actualVersion, err := codec.ReadLatest(s, f.Decode)
		// then
		assert.NoError(t, err)
		assert.True(t, secondVersion.Time.Equal(actualVersion.Time))
		assert.Equal(t, second, f.DataRead())
	})

	t.Run("should read previous version of data when last one is corrupted", func(t *testing.T) {
		dir := tests.TempDir(t)
		s, err := store.Open(dir)
		require.NoError(t, err)
		firstData := []byte("data")
		firstVersion := tests.WriteData(t, s, firstData)

		dirCopy := tests.TempDir(t)
		err = otiai10.Copy(dir, dirCopy)
		require.NoError(t, err)

		tests.WriteData(t, s, []byte("second version"))
		tests.CorruptFiles(t, dir)       // we don't know which files to corrupt therefore all will be corrupted
		err = otiai10.Copy(dirCopy, dir) // bring back files which were not corrupted
		require.NoError(t, err)
		f := &tests.FakeDecoder{}
		// when
		actualVersion, err := codec.ReadLatest(s, f.Decode)
		// then
		assert.NoError(t, err)
		assert.True(t, firstVersion.Time.Equal(actualVersion.Time))
		assert.Equal(t, firstData, f.DataRead())
	})
}

func failingDecoder(io.Reader) error {
	return errors.New("decoder failed")
}
