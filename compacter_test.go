package deebee_test

import (
	"context"
	"testing"
	"time"

	"github.com/jacekolszak/deebee"
	"github.com/jacekolszak/deebee/fake"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompacter(t *testing.T) {
	const state = "state"

	t.Run("should open database with custom compacter", func(t *testing.T) {
		compacter := func(ctx context.Context, state deebee.State) {}
		db, err := deebee.Open(fake.ExistingDir(), deebee.Compacter(compacter))
		require.NoError(t, err)
		assert.NotNil(t, db)
	})

	t.Run("compacter should be executed when database is open", func(t *testing.T) {
		var contextReceived context.Context
		var stateReceived deebee.State
		compacter := func(ctx context.Context, state deebee.State) {
			contextReceived = ctx
			stateReceived = state
		}
		// when
		openDbWithCompacter(t, compacter)
		// then
		require.NotNil(t, contextReceived)
		require.NotNil(t, stateReceived)
		assert.NotNil(t, stateReceived.Updates())
	})

	t.Run("Updates channel should inform compacter that state was changed", func(t *testing.T) {
		var updates <-chan struct{}
		compacter := func(ctx context.Context, state deebee.State) {
			updates = state.Updates()
		}
		db := openDbWithCompacter(t, compacter)
		writeData(t, db, state, []byte("new"))
		// when
		writeData(t, db, state, []byte("updated"))
		// then
		assertUpdateReceived(t, updates)
	})

	t.Run("should cancel context passed to compacter when database is closed", func(t *testing.T) {
		var contextReceived context.Context
		compacter := func(ctx context.Context, state deebee.State) {
			contextReceived = ctx
		}
		db := openDbWithCompacter(t, compacter)
		// when
		err := db.Close()
		// then
		require.NoError(t, err)
		assertClosed(t, contextReceived.Done())
	})

	t.Run("should cancel updates channel passed to compacter when database is closed", func(t *testing.T) {
		var updates <-chan struct{}
		compacter := func(ctx context.Context, state deebee.State) {
			updates = state.Updates()
		}
		db := openDbWithCompacter(t, compacter)
		// when
		err := db.Close()
		// then
		require.NoError(t, err)
		assertClosed(t, updates)
	})
}

func openDbWithCompacter(t *testing.T, compacter deebee.CompactState) *deebee.DB {
	db, err := deebee.Open(fake.ExistingDir(), deebee.Compacter(compacter))
	require.NoError(t, err)
	assert.NotNil(t, db)
	return db
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
