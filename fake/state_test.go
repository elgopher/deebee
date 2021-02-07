package fake_test

import (
	"testing"
	"time"

	"github.com/jacekolszak/deebee/fake"
	"github.com/jacekolszak/deebee/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestState_Versions(t *testing.T) {
	t.Run("should return empty versions", func(t *testing.T) {
		state := &fake.State{}
		versions, err := state.Versions()
		require.NoError(t, err)
		assert.Empty(t, versions)
	})
}

func TestState_AddVersion(t *testing.T) {
	t.Run("should add one version", func(t *testing.T) {
		state := &fake.State{}
		// when
		state.AddVersion(1)
		// then
		assertStateVersions(t, []int{1}, state)
		assertUpdateReceived(t, state.Updates())
	})

	t.Run("should add two versions", func(t *testing.T) {
		state := &fake.State{}
		// when
		state.AddVersion(1)
		assertUpdateReceived(t, state.Updates())
		state.AddVersion(2)
		// then
		assertStateVersions(t, []int{1, 2}, state)
		assertUpdateReceived(t, state.Updates())
	})
}

func TestStateVersion_Remove(t *testing.T) {
	t.Run("should remove only one remaining version", func(t *testing.T) {
		state := &fake.State{}
		state.AddVersion(1)
		// when
		versions, err := state.Versions()
		require.NoError(t, err)
		require.Len(t, versions, 1)
		err = versions[0].Remove()
		require.NoError(t, err)
		// then
		assertStateVersions(t, []int{}, state)
	})

	t.Run("should remove one out two", func(t *testing.T) {
		state := &fake.State{}
		state.AddVersion(1)
		state.AddVersion(2)
		// when
		versions, err := state.Versions()
		require.NoError(t, err)
		require.Len(t, versions, 2)
		err = versions[0].Remove()
		require.NoError(t, err)
		// then
		assertStateVersions(t, []int{2}, state)
	})
}

func TestState_Revisions(t *testing.T) {
	t.Run("should return empty", func(t *testing.T) {
		state := &fake.State{}
		revisions := state.Revisions()
		assert.Empty(t, revisions)
	})

	t.Run("should return one revision", func(t *testing.T) {
		state := &fake.State{}
		state.AddVersion(1)
		revisions := state.Revisions()
		assert.Equal(t, []int{1}, revisions)
	})

	t.Run("should return two revisions", func(t *testing.T) {
		state := &fake.State{}
		state.AddVersion(1)
		state.AddVersion(2)
		revisions := state.Revisions()
		assert.Equal(t, []int{1, 2}, revisions)
	})
}

func TestState_ThreadSafety(t *testing.T) {
	t.Run("test with --race flag should not report data races", func(t *testing.T) {
		state := &fake.State{}
		for i := 0; i < 100; i++ {
			go state.AddVersion(i)
			go func() { _, _ = state.Versions() }()
			go func() {
				versions, err := state.Versions()
				require.NoError(t, err)
				if len(versions) > 0 {
					version := versions[0]
					version.Revision()
					require.NoError(t, version.Remove())
				}
			}()
			go state.Revisions()
			go state.Updates()
		}
	})
}

func assertStateVersions(t *testing.T, expectedRevisions []int, s store.State) {
	versions, err := s.Versions()
	require.NoError(t, err)
	assert.Len(t, versions, len(expectedRevisions))
	for i, revision := range expectedRevisions {
		assertRevision(t, revision, versions[i])
	}
}

func assertRevision(t *testing.T, expectedRevision int, version store.StateVersion) {
	assert.Equal(t, expectedRevision, version.Revision())
}

func assertUpdateReceived(t *testing.T, updates <-chan struct{}) {
	select {
	case _, ok := <-updates:
		assert.True(t, ok)
	case <-time.After(1 * time.Second):
		assert.FailNow(t, "timeout waiting for update")
	}
}
