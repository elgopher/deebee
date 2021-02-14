package fake_test

import (
	"testing"
	"time"

	"github.com/jacekolszak/deebee/fake"
	"github.com/jacekolszak/deebee/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var time1 = time.Unix(1000000000, 0)
var time2 = time.Unix(2000000000, 0)

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
		assertStateRevisions(t, []int{1}, state)
		assertUpdateReceived(t, state.Updates())
	})

	t.Run("should add two versions", func(t *testing.T) {
		state := &fake.State{}
		// when
		state.AddVersion(1)
		assertUpdateReceived(t, state.Updates())
		state.AddVersion(2)
		// then
		assertStateRevisions(t, []int{1, 2}, state)
		assertUpdateReceived(t, state.Updates())
	})
}

func TestState_AddVersionWithTime(t *testing.T) {
	t.Run("should add one version", func(t *testing.T) {
		state := &fake.State{}
		// when
		state.AddVersionWithTime(1, time1)
		// then
		assertStates(t, []expectedState{
			{
				revision: 1,
				time:     time1,
			},
		}, state)
		assertUpdateReceived(t, state.Updates())
	})

	t.Run("should add two versions", func(t *testing.T) {
		state := &fake.State{}
		expectedStates := []expectedState{
			{
				revision: 1,
				time:     time1,
			},
			{
				revision: 2,
				time:     time2,
			},
		}

		// when
		state.AddVersionWithTime(expectedStates[0].revision, expectedStates[0].time)
		state.AddVersionWithTime(expectedStates[1].revision, expectedStates[1].time)
		// then
		assertStates(t, expectedStates, state)
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
		assertStateRevisions(t, []int{}, state)
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
		assertStateRevisions(t, []int{2}, state)
	})

	t.Run("should remove version added using AddVersionWithTime", func(t *testing.T) {
		state := &fake.State{}
		state.AddVersionWithTime(1, time1)
		// when
		versions, err := state.Versions()
		require.NoError(t, err)
		require.Len(t, versions, 1)
		err = versions[0].Remove()
		require.NoError(t, err)
		// then
		assertStateRevisions(t, []int{}, state)
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

func TestState_Close(t *testing.T) {
	t.Run("should close newly created state", func(t *testing.T) {
		state := fake.State{}
		// when
		state.Close()
		// then
		assertClosed(t, state.Updates())
	})

	t.Run("should close state with already consumed updates", func(t *testing.T) {
		state := fake.State{}
		state.AddVersion(1)
		<-state.Updates()
		// when
		state.Close()
		// then
		assertClosed(t, state.Updates())
	})

	t.Run("close should be idempotent", func(t *testing.T) {
		state := fake.State{}
		state.Close()
		// when
		state.Close()
		// then
		assertClosed(t, state.Updates())
	})

	t.Run("close should disable AddVersion", func(t *testing.T) {
		state := fake.State{}
		state.Close()
		assert.Panics(t, func() {
			state.AddVersion(1)
		})
	})

	t.Run("close should disable AddVersionWithTime", func(t *testing.T) {
		state := fake.State{}
		state.Close()
		assert.Panics(t, func() {
			state.AddVersionWithTime(1, time1)
		})
	})
}

func TestState_ThreadSafety(t *testing.T) {
	t.Run("test with --race flag should not report data races", func(t *testing.T) {
		state := &fake.State{}
		for i := 0; i < 100; i++ {
			go state.AddVersion(i)
			go state.AddVersionWithTime(i, time1)
			go func() { _, _ = state.Versions() }()
			go func() {
				versions, err := state.Versions()
				require.NoError(t, err)
				if len(versions) > 0 {
					version := versions[0]
					version.Revision()
					version.Time()
					require.NoError(t, version.Remove())
				}
			}()
			go state.Revisions()
			go state.Updates()
		}
	})

	t.Run("for state.Close(), test with --race flag should not report data races", func(t *testing.T) {
		state := &fake.State{}
		for i := 0; i < 100; i++ {
			go state.Close()
		}
	})
}

func assertStateRevisions(t *testing.T, expectedRevisions []int, s store.State) {
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

type expectedState struct {
	revision int
	time     time.Time
}

func assertStates(t *testing.T, expectedStates []expectedState, s store.State) {
	versions, err := s.Versions()
	require.NoError(t, err)
	assert.Len(t, versions, len(expectedStates))
	for i, expected := range expectedStates {
		assert.Equal(t, expected.time, versions[i].Time())
		assert.Equal(t, expected.revision, versions[i].Revision())
	}
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
