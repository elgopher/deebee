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
		from := &storeMock{reader: &readerFailingOnRead{}}
		to := &storeMock{writer: &writerMock{}}
		// when
		err := replicator.CopyFromTo(from, to)
		// then
		require.Error(t, err)
		assert.True(t, to.writer.aborted, "writer was not aborted")
	})

	t.Run("should abort writer when reader.Close returned error", func(t *testing.T) {
		from := &storeMock{reader: &readerFailingOnClose{}}
		to := &storeMock{writer: &writerMock{}}
		// when
		err := replicator.CopyFromTo(from, to)
		// then
		require.Error(t, err)
		assert.True(t, to.writer.aborted, "writer was not aborted")
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

type storeMock struct {
	reader store.Reader
	writer *writerMock
}

func (s *storeMock) Reader(...store.ReaderOption) (store.Reader, error) {
	return s.reader, nil
}

func (s *storeMock) Versions() ([]store.Version, error) {
	return nil, nil
}

func (s *storeMock) Writer(...store.WriterOption) (store.Writer, error) {
	return s.writer, nil
}

type readerMock struct{}

func (r *readerMock) Read(p []byte) (n int, err error) {
	return 0, io.EOF
}

func (r *readerMock) Close() error {
	return nil
}

func (r *readerMock) Version() store.Version {
	return store.Version{}
}

type readerFailingOnRead struct {
	readerMock
}

func (r *readerFailingOnRead) Read([]byte) (n int, err error) {
	return 0, errors.New("error")
}

type readerFailingOnClose struct {
	readerMock
}

func (r *readerFailingOnClose) Close() error {
	return errors.New("error")
}

type writerMock struct {
	aborted bool
}

func (w *writerMock) Write(p []byte) (n int, err error) {
	return len(p), nil
}

func (w *writerMock) Close() error {
	return nil
}

func (w *writerMock) Version() store.Version {
	return store.Version{}
}

func (w *writerMock) AbortAndClose() {
	w.aborted = true
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
