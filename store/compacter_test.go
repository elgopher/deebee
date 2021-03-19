// (c) 2021 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/jacekolszak/deebee/fake"
	"github.com/jacekolszak/deebee/internal/storetest"
	"github.com/jacekolszak/deebee/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompacter(t *testing.T) {
	t.Run("should open store with custom compacter", func(t *testing.T) {
		compacter := func(ctx context.Context, state store.State) {}
		s, err := store.Open(fake.ExistingDir(), store.Compacter(compacter))
		require.NoError(t, err)
		assert.NotNil(t, s)
	})

	t.Run("compacter should be executed when store is open", func(t *testing.T) {
		var contextReceived context.Context
		var stateReceived store.State
		compacter := func(ctx context.Context, state store.State) {
			contextReceived = ctx
			stateReceived = state
		}
		// when
		openStoreWithCompacter(t, compacter)
		// then
		require.NotNil(t, contextReceived)
		require.NotNil(t, stateReceived)
		assert.NotNil(t, stateReceived.Updates())
	})

	t.Run("Updates channel should inform compacter that state was changed", func(t *testing.T) {
		var updates <-chan struct{}
		compacter := func(ctx context.Context, state store.State) {
			updates = state.Updates()
		}
		s := openStoreWithCompacter(t, compacter)
		storetest.WriteData(t, s, []byte("new"))
		// when
		storetest.WriteData(t, s, []byte("updated"))
		// then
		assertUpdateReceived(t, updates)
	})

	t.Run("should cancel context passed to compacter when store is closed", func(t *testing.T) {
		var contextReceived context.Context
		compacter := func(ctx context.Context, state store.State) {
			contextReceived = ctx
		}
		s := openStoreWithCompacter(t, compacter)
		// when
		err := s.Close()
		// then
		require.NoError(t, err)
		assertClosed(t, contextReceived.Done())
	})

	t.Run("should cancel updates channel passed to compacter when store is closed", func(t *testing.T) {
		var updates <-chan struct{}
		compacter := func(ctx context.Context, state store.State) {
			updates = state.Updates()
		}
		s := openStoreWithCompacter(t, compacter)
		// when
		err := s.Close()
		// then
		require.NoError(t, err)
		assertClosed(t, updates)
	})

	t.Run("store.Close() should wait for compacter to finish", func(t *testing.T) {
		var finished bool
		compacter := func(ctx context.Context, state store.State) {
			<-ctx.Done()
			finished = true
		}
		s := openStoreWithOptions(t, store.Compacter(compacter))
		// when
		err := s.Close()
		require.NoError(t, err)
		// then
		assert.True(t, finished)
	})
}

type fakeNow struct {
	currentTime time.Time
}

func (t *fakeNow) Now() time.Time {
	return t.currentTime
}

func openStoreWithCompacter(t *testing.T, compacter store.CompactState) *store.Store {
	return openStoreWithCompacterAndDir(t, compacter, fake.ExistingDir())
}

func openStoreWithCompacterAndDir(t *testing.T, compacter store.CompactState, dir store.Dir) *store.Store {
	compacterFinished := make(chan struct{})
	s, err := store.Open(dir, store.Compacter(func(ctx context.Context, state store.State) {
		compacter(ctx, state)
		close(compacterFinished)
	}))
	require.NoError(t, err)
	assert.NotNil(t, s)
	requireClosed(t, compacterFinished)
	return s
}

func openStoreWithOptions(t *testing.T, options ...store.Option) *store.Store {
	s, err := store.Open(fake.ExistingDir(), options...)
	require.NoError(t, err)
	assert.NotNil(t, s)
	return s
}

func assertUpdateReceived(t *testing.T, updates <-chan struct{}) {
	select {
	case _, ok := <-updates:
		assert.True(t, ok)
	case <-time.After(1 * time.Second):
		assert.FailNow(t, "timeout waiting for update")
	}
}

func assertClosed(t *testing.T, channel <-chan struct{}) {
	select {
	case _, ok := <-channel:
		assert.False(t, ok, "channel not closed")
	case <-time.After(1 * time.Second):
		assert.FailNow(t, "timeout waiting for close")
	}
}

func requireClosed(t *testing.T, channel <-chan struct{}) {
	select {
	case _, ok := <-channel:
		require.False(t, ok, "channel not closed")
	case <-time.After(1 * time.Second):
		require.FailNow(t, "timeout waiting for close")
	}
}
