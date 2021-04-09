// (c) 2021 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

package compacter_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jacekolszak/deebee/compacter"
	"github.com/jacekolszak/deebee/internal/tests"
	"github.com/jacekolszak/deebee/store"
	otiai10 "github.com/otiai10/copy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRunOnce(t *testing.T) {

	t.Run("should return error for nil store", func(t *testing.T) {
		err := compacter.RunOnce(nil)
		assert.Error(t, err)
	})

	t.Run("should return error when option returned error", func(t *testing.T) {
		s := tests.OpenStore(t)
		option := func(options *compacter.Options) error {
			return errors.New("error")
		}
		// when
		err := compacter.RunOnce(s, option)
		// then
		assert.Error(t, err)
	})

	t.Run("should retain latest version by default", func(t *testing.T) {
		s := tests.OpenStore(t)
		tests.WriteData(t, s, []byte("v1"))
		tests.WriteData(t, s, []byte("v2"))
		err := compacter.RunOnce(s)
		assert.NoError(t, err)
		versions, err := s.Versions()
		require.NoError(t, err)
		assert.Len(t, versions, 1)
	})

	t.Run("should remove all versions up to latest integral one", func(t *testing.T) {
		s := storeWithLastVersionCorrupted(t)
		// when
		err := compacter.RunOnce(s)
		assert.NoError(t, err)
		// then
		versions, err := s.Versions()
		require.NoError(t, err)
		assert.Len(t, versions, 2) // one integral and one corrupted
	})
}

func TestStart(t *testing.T) {

	t.Run("should return error for nil store", func(t *testing.T) {
		err := compacter.Start(context.Background(), nil)
		assert.Error(t, err)
	})

	t.Run("should stop once context is cancelled", func(t *testing.T) {
		s := tests.OpenStore(t)
		ctx, cancel := context.WithCancel(context.Background())
		var err error
		async := tests.RunAsync(func() {
			err = compacter.Start(ctx, s)
		})
		// when
		cancel()
		// then
		async.WaitOrFailAfter(t, time.Second)
		assert.NoError(t, err)
	})

	t.Run("should skip nil option", func(t *testing.T) {
		s := tests.OpenStore(t)
		ctx, cancel := context.WithCancel(context.Background())
		var err error
		async := tests.RunAsync(func() {
			err = compacter.Start(ctx, s)
		})
		cancel()
		async.WaitOrFailAfter(t, time.Second)
		assert.NoError(t, err)
	})

	t.Run("should return error when option returned error", func(t *testing.T) {
		s := tests.OpenStore(t)
		option := func(options *compacter.Options) error {
			return errors.New("error")
		}
		// when
		err := compacter.Start(context.Background(), s, option)
		// then
		assert.Error(t, err)
	})

	t.Run("should continuously compact versions in the background", func(t *testing.T) {
		s := tests.OpenStore(t)
		ctx, cancel := context.WithCancel(context.Background())
		async := tests.RunAsync(func() {
			_ = compacter.Start(ctx, s, compacter.Interval(time.Millisecond))
		})
		// when
		tests.WriteData(t, s, []byte("v1"))
		tests.WriteData(t, s, []byte("v2"))
		// then
		assert.Eventually(t, numberOfVersions(s, 1), 100*time.Millisecond, time.Millisecond)
		// and when
		tests.WriteData(t, s, []byte("v3"))
		// then
		assert.Eventually(t, numberOfVersions(s, 1), 100*time.Millisecond, time.Millisecond)
		// cleanup
		cancel()
		async.WaitOrFailAfter(t, time.Second)
	})
}

func storeWithLastVersionCorrupted(t *testing.T) *store.Store {
	dir := tests.TempDir(t)
	s, err := store.Open(dir)
	require.NoError(t, err)
	notCorruptedDir := tests.TempDir(t)
	tests.WriteData(t, s, []byte("not corrupted 1"))
	tests.WriteData(t, s, []byte("not corrupted 2"))
	err = otiai10.Copy(dir, notCorruptedDir)
	require.NoError(t, err)

	tests.WriteData(t, s, []byte("corrupted"))
	tests.CorruptFiles(t, dir)

	err = otiai10.Copy(notCorruptedDir, dir)
	require.NoError(t, err)
	return s
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
