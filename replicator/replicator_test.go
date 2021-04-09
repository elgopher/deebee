// (c) 2021 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

package replicator_test

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/jacekolszak/deebee/internal/tests"
	"github.com/jacekolszak/deebee/replicator"
	"github.com/jacekolszak/deebee/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCopyFromTo(t *testing.T) {

	t.Run("should return error when from is nil", func(t *testing.T) {
		err := replicator.CopyFromTo(nil, tests.OpenStore(t))
		assert.Error(t, err)
	})

	t.Run("should return error when to is nil", func(t *testing.T) {
		err := replicator.CopyFromTo(tests.OpenStore(t), nil)
		assert.Error(t, err)
	})

	t.Run("should return error when no version was found", func(t *testing.T) {
		from, to := tests.OpenStore(t), tests.OpenStore(t)
		err := replicator.CopyFromTo(from, to)
		assert.True(t, store.IsVersionNotFound(err))
	})

	t.Run("should copy latest version preserving the time", func(t *testing.T) {
		from, to := tests.OpenStore(t), tests.OpenStore(t)
		tests.WriteData(t, from, []byte("v1"))
		data := []byte("v2")
		v2 := tests.WriteData(t, from, data)
		// when
		err := replicator.CopyFromTo(from, to)
		// then
		require.NoError(t, err)
		dataRead := tests.ReadData(t, to, store.Time(v2.Time))
		assert.Equal(t, data, dataRead)
	})

	t.Run("should abort writer when reader.Read returned error", func(t *testing.T) {
		from := &tests.StoreMock{ReturnReader: &tests.ReaderFailingOnRead{}}
		writer := &tests.WriterMock{}
		to := &tests.StoreMock{ReturnWriter: writer}
		// when
		err := replicator.CopyFromTo(from, to)
		// then
		require.Error(t, err)
		assert.True(t, writer.IsAborted(), "writer was not aborted")
	})

	t.Run("should abort writer when reader.Close returned error", func(t *testing.T) {
		from := &tests.StoreMock{ReturnReader: &tests.ReaderFailingOnClose{}}
		writer := &tests.WriterMock{}
		to := &tests.StoreMock{ReturnWriter: writer}
		// when
		err := replicator.CopyFromTo(from, to)
		// then
		require.Error(t, err)
		assert.True(t, writer.IsAborted(), "writer was not aborted")
	})

}

func TestStartFromTo(t *testing.T) {

	t.Run("should return error when from is nil", func(t *testing.T) {
		err := replicator.StartFromTo(context.Background(), nil, tests.OpenStore(t))
		assert.Error(t, err)
	})

	t.Run("should return error when to is nil", func(t *testing.T) {
		err := replicator.StartFromTo(context.Background(), tests.OpenStore(t), nil)
		assert.Error(t, err)
	})

	t.Run("should stop once context is cancelled", func(t *testing.T) {
		from, to := tests.OpenStore(t), tests.OpenStore(t)
		ctx, cancel := context.WithCancel(context.Background())
		var err error
		async := tests.RunAsync(func() {
			err = replicator.StartFromTo(ctx, from, to)
		})
		// when
		cancel()
		// then
		async.WaitOrFailAfter(t, time.Second)
		assert.NoError(t, err)
	})

	t.Run("should accept nil option", func(t *testing.T) {
		from, to := tests.OpenStore(t), tests.OpenStore(t)
		ctx, cancel := context.WithCancel(context.Background())
		var err error
		async := tests.RunAsync(func() {
			// when
			err = replicator.StartFromTo(ctx, from, to, nil)
		})
		cancel()
		async.WaitOrFailAfter(t, time.Second)
		// then
		assert.NoError(t, err)
	})

	t.Run("should return error when option returned error", func(t *testing.T) {
		from, to := tests.OpenStore(t), tests.OpenStore(t)
		option := func(*replicator.Options) error {
			return errors.New("error")
		}
		err := replicator.StartFromTo(context.Background(), from, to, option)
		assert.Error(t, err)
	})

	t.Run("should continuously copy files in the background", func(t *testing.T) {
		from, to := tests.OpenStore(t), tests.OpenStore(t)
		ctx, cancel := context.WithCancel(context.Background())
		async := tests.RunAsync(func() {
			_ = replicator.StartFromTo(ctx, from, to, replicator.Interval(time.Millisecond))
		})
		// when
		tests.WriteData(t, from, []byte("v1"))
		// then
		assert.Eventually(t, numberOfVersions(to, 1), 100*time.Millisecond, time.Millisecond)
		// and when
		tests.WriteData(t, from, []byte("v2"))
		// then
		assert.Eventually(t, numberOfVersions(to, 2), 100*time.Millisecond, time.Millisecond)
		// cleanup
		cancel()
		async.WaitOrFailAfter(t, time.Second)
	})
}

func TestReadLatest(t *testing.T) {
	t.Run("should return error", func(t *testing.T) {
		t.Run("when no store is given", func(t *testing.T) {
			decoder := func(reader io.Reader) error {
				return nil
			}
			_, err := replicator.ReadLatest(decoder)
			assert.Error(t, err)
		})

		t.Run("when no decoder is given", func(t *testing.T) {
			s := tests.OpenStore(t)
			_, err := replicator.ReadLatest(nil, s)
			assert.Error(t, err)
		})

		t.Run("nil stores", func(t *testing.T) {
			decoder := func(reader io.Reader) error {
				return nil
			}
			_, err := replicator.ReadLatest(decoder, nil, nil)
			assert.Error(t, err)
		})

		t.Run("when stores are empty", func(t *testing.T) {
			s := &tests.StoreMock{
				ReturnVersions: nil,
			}
			f := &tests.FakeDecoder{}
			_, err := replicator.ReadLatest(f.Decode, s, s)
			assert.True(t, store.IsVersionNotFound(err))
		})

		t.Run("when Store.Versions returned error for all stores", func(t *testing.T) {
			e := errors.New("listing versions failed")
			s := &tests.StoreMock{ReturnVersionsError: e}
			f := &tests.FakeDecoder{}
			_, err := replicator.ReadLatest(f.Decode, s, s)
			assert.True(t, store.IsVersionNotFound(err))
		})

		t.Run("when Store.Reader returned error for all stores", func(t *testing.T) {
			s := &tests.StoreMock{
				ReturnVersions:    []store.Version{{}},
				ReturnReaderError: errors.New("opening reader failed"),
			}
			f := &tests.FakeDecoder{}
			_, err := replicator.ReadLatest(f.Decode, s, s)
			assert.True(t, store.IsVersionNotFound(err))
		})

		t.Run("when decoder returned error for all stores", func(t *testing.T) {
			s := &tests.StoreMock{
				ReturnVersions: []store.Version{{}},
				ReturnReader:   &tests.ReaderMock{},
			}
			_, err := replicator.ReadLatest(failingDecoder, s, s)
			assert.True(t, store.IsVersionNotFound(err))
		})
	})

	t.Run("should pick latest from one store", func(t *testing.T) {
		s := tests.OpenStore(t)
		tests.WriteData(t, s, []byte("1"))
		data := []byte("2")
		v := tests.WriteData(t, s, data)
		decoder := &tests.FakeDecoder{}
		// when
		actualVersion, err := replicator.ReadLatest(decoder.Decode, s)
		// then
		require.NoError(t, err)
		assert.True(t, v.Time.Equal(actualVersion.Time))
		assert.Equal(t, data, decoder.DataRead())
	})

	t.Run("should pick from first store when two stores have identical version time", func(t *testing.T) {
		s1 := tests.OpenStore(t)
		data := []byte("data")
		v := tests.WriteData(t, s1, data)

		s2 := tests.OpenStore(t)
		tests.WriteData(t, s2, []byte("other"), store.WriteTime(v.Time))

		decoder := &tests.FakeDecoder{}
		// when
		actualVersion, err := replicator.ReadLatest(decoder.Decode, s1, s2)
		// then
		require.NoError(t, err)
		assert.True(t, v.Time.Equal(actualVersion.Time))
		assert.Equal(t, data, decoder.DataRead())
	})

	t.Run("should pick from second store which has more recent version", func(t *testing.T) {
		now := time.Now()
		secondLater := now.Add(time.Second)

		s1 := tests.OpenStore(t)
		tests.WriteData(t, s1, []byte("1"), store.WriteTime(now))
		s2 := tests.OpenStore(t)
		data := []byte("2")
		v := tests.WriteData(t, s2, data, store.WriteTime(secondLater))

		decoder := &tests.FakeDecoder{}
		// when
		actualVersion, err := replicator.ReadLatest(decoder.Decode, s1, s2)
		// then
		require.NoError(t, err)
		assert.True(t, v.Time.Equal(actualVersion.Time))
		assert.Equal(t, data, decoder.DataRead())
	})

	t.Run("should pick from second store when data in first is corrupted", func(t *testing.T) {
		now := time.Now()
		s1dir := tests.TempDir(t)
		s1, _ := store.Open(s1dir)
		tests.WriteData(t, s1, []byte("1"), store.WriteTime(now))
		tests.CorruptFiles(t, s1dir)

		s2 := tests.OpenStore(t)
		data := []byte("2")
		tests.WriteData(t, s2, data, store.WriteTime(now))

		decoder := &tests.FakeDecoder{}
		// when
		actualVersion, err := replicator.ReadLatest(decoder.Decode, s1, s2)
		// then
		require.NoError(t, err)
		assert.True(t, now.Equal(actualVersion.Time))
		assert.Equal(t, data, decoder.DataRead())

	})
}

func failingDecoder(io.Reader) error {
	return errors.New("decoder failed")
}

func numberOfVersions(s *store.Store, l int) func() bool {
	return func() bool {
		versions, err := s.Versions()
		if err != nil {
			panic(err)
		}
		return len(versions) == l
	}
}
