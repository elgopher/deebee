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
		writeData(t, db, []byte("new"))
		// when
		writeData(t, db, []byte("updated"))
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

func TestState_Versions(t *testing.T) {
	t.Run("should return empty state versions", func(t *testing.T) {
		var state deebee.State
		compacter := func(ctx context.Context, s deebee.State) {
			state = s
		}
		openDbWithCompacter(t, compacter)
		// when
		versions, err := state.Versions()
		require.NoError(t, err)
		assert.Empty(t, versions)
	})

	t.Run("should return one state version", func(t *testing.T) {
		var state deebee.State
		compacter := func(ctx context.Context, s deebee.State) {
			state = s
		}
		db := openDbWithCompacter(t, compacter)
		writeData(t, db, []byte("data"))
		// when
		states, err := state.Versions()
		require.NoError(t, err)
		require.Len(t, states, 1)
	})

	t.Run("should return two state versions", func(t *testing.T) {
		var state deebee.State
		compacter := func(ctx context.Context, s deebee.State) {
			state = s
		}
		db := openDbWithCompacter(t, compacter)
		writeData(t, db, []byte("data"))
		writeData(t, db, []byte("updated"))
		// when
		states, err := state.Versions()
		require.NoError(t, err)
		require.Len(t, states, 2)
		assert.True(t, states[0].Revision != states[1].Revision, "revisions are not different")
	})

	t.Run("should return sorted states by revision", func(t *testing.T) {
		var state deebee.State
		compacter := func(ctx context.Context, s deebee.State) {
			state = s
		}
		db := openDbWithCompacter(t, compacter)
		const revisions = 256
		for i := 0; i < revisions; i++ {
			writeData(t, db, []byte("data"))
		}
		// when
		states, err := state.Versions()
		require.NoError(t, err)
		require.Len(t, states, revisions)
		for i := 0; i < revisions-1; i++ {
			assert.True(t, states[i].Revision < states[i+1].Revision, "revisions are not sorted: states[%d].Revision < states[%d].Revision", i, i+1)
		}
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
