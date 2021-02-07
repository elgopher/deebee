package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/jacekolszak/deebee/failing"
	"github.com/jacekolszak/deebee/fake"
	"github.com/jacekolszak/deebee/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompacter(t *testing.T) {
	t.Run("should open database with custom compacter", func(t *testing.T) {
		compacter := func(ctx context.Context, state store.State) {}
		db, err := store.Open(fake.ExistingDir(), store.Compacter(compacter))
		require.NoError(t, err)
		assert.NotNil(t, db)
	})

	t.Run("compacter should be executed when database is open", func(t *testing.T) {
		var contextReceived context.Context
		var stateReceived store.State
		compacter := func(ctx context.Context, state store.State) {
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
		compacter := func(ctx context.Context, state store.State) {
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
		compacter := func(ctx context.Context, state store.State) {
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
		compacter := func(ctx context.Context, state store.State) {
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
		var state store.State
		compacter := func(ctx context.Context, s store.State) {
			state = s
		}
		openDbWithCompacter(t, compacter)
		// when
		versions, err := state.Versions()
		require.NoError(t, err)
		assert.Empty(t, versions)
	})

	t.Run("should return one state version", func(t *testing.T) {
		var state store.State
		compacter := func(ctx context.Context, s store.State) {
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
		var state store.State
		compacter := func(ctx context.Context, s store.State) {
			state = s
		}
		db := openDbWithCompacter(t, compacter)
		writeData(t, db, []byte("data"))
		writeData(t, db, []byte("updated"))
		// when
		states, err := state.Versions()
		require.NoError(t, err)
		require.Len(t, states, 2)
		assert.True(t, states[0].Revision() != states[1].Revision(), "revisions are not different")
	})

	t.Run("should return sorted states by revision", func(t *testing.T) {
		var state store.State
		compacter := func(ctx context.Context, s store.State) {
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
			assert.True(t, states[i].Revision() < states[i+1].Revision(), "revisions are not sorted: states[%d].Revision < states[%d].Revision", i, i+1)
		}
	})

	t.Run("should return time of state creation", func(t *testing.T) {
		creationTime, err := time.Parse(time.RFC3339, "1999-01-01T12:00:00Z")
		require.NoError(t, err)
		time2, err := time.Parse(time.RFC3339, "2077-01-01T12:00:00Z")
		require.NoError(t, err)

		var state store.State
		compacter := func(ctx context.Context, s store.State) {
			state = s
		}
		fakeTime := &fakeNow{currentTime: creationTime}
		db := openDbWithOptions(t, store.Compacter(compacter), store.Now(fakeTime.Now))
		writeData(t, db, []byte("data"))
		// when
		fakeTime.currentTime = time2
		states, err := state.Versions()
		// then
		require.NoError(t, err)
		require.Len(t, states, 1)
		assert.Equal(t, creationTime, states[0].Time())
	})
}

type fakeNow struct {
	currentTime time.Time
}

func (t *fakeNow) Now() time.Time {
	return t.currentTime
}

func TestState_Remove(t *testing.T) {
	t.Run("should return empty states when last remaining version is removed", func(t *testing.T) {
		var state store.State
		compacter := func(ctx context.Context, s store.State) {
			state = s
		}
		db := openDbWithCompacter(t, compacter)
		writeData(t, db, []byte("data"))
		states, err := state.Versions()
		require.NoError(t, err)
		// when
		err = states[0].Remove()
		// then
		require.NoError(t, err)
		states, err = state.Versions()
		require.NoError(t, err)
		assert.Empty(t, states)
	})

	t.Run("should remove one state's version when two versions are available", func(t *testing.T) {
		var state store.State
		compacter := func(ctx context.Context, s store.State) {
			state = s
		}
		db := openDbWithCompacter(t, compacter)
		writeData(t, db, []byte("data1"))
		writeData(t, db, []byte("data2"))
		states, err := state.Versions()
		require.NoError(t, err)
		removedState := states[0]
		// when
		err = removedState.Remove()
		// then
		require.NoError(t, err)
		states, err = state.Versions()
		require.NoError(t, err)
		assert.Len(t, states, 1)
		assert.NotEqual(t, removedState.Revision(), states[0].Revision(), "wrong revision removed")
	})

	t.Run("should return error when dir.DeleteFile is failing", func(t *testing.T) {
		var state store.State
		compacter := func(ctx context.Context, s store.State) {
			state = s
		}
		dir := failing.DeleteFile(fake.ExistingDir())
		db := openDbWithCompacterAndDir(t, compacter, dir)

		writeData(t, db, []byte("data1"))
		states, err := state.Versions()
		require.NoError(t, err)
		// when
		err = states[0].Remove()
		// then
		assert.Error(t, err)
	})
}

func openDbWithCompacter(t *testing.T, compacter store.CompactState) *store.DB {
	return openDbWithCompacterAndDir(t, compacter, fake.ExistingDir())
}

func openDbWithCompacterAndDir(t *testing.T, compacter store.CompactState, dir store.Dir) *store.DB {
	db, err := store.Open(dir, store.Compacter(compacter))
	require.NoError(t, err)
	assert.NotNil(t, db)
	return db
}

func openDbWithOptions(t *testing.T, options ...store.Option) *store.DB {
	db, err := store.Open(fake.ExistingDir(), options...)
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