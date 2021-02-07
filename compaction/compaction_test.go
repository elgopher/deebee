package compaction_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jacekolszak/deebee/compaction"
	"github.com/jacekolszak/deebee/failing"
	"github.com/jacekolszak/deebee/fake"
	"github.com/jacekolszak/deebee/store"
	"github.com/jacekolszak/deebee/storetest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStrategy(t *testing.T) {
	t.Run("should return error when StrategyOption returned error", func(t *testing.T) {
		strategy := compaction.Strategy(func(*compaction.Compacter) error {
			return errors.New("error")
		})
		_, err := store.Open(fake.ExistingDir(), strategy)
		assert.Error(t, err)
	})

	t.Run("should accept strategy with default options", func(t *testing.T) {
		defaultStrategy := compaction.Strategy()
		_, err := store.Open(fake.ExistingDir(), defaultStrategy)
		assert.NoError(t, err)
	})

	t.Run("should create Compacter instance and pass it to StrategyOption", func(t *testing.T) {
		var compactionStrategyReceived *compaction.Compacter
		strategy := compaction.Strategy(func(s *compaction.Compacter) error {
			compactionStrategyReceived = s
			return nil
		})
		// when
		openStore(t, strategy)
		// then
		assert.NotNil(t, compactionStrategyReceived)
	})

	t.Run("should skip nil StrategyOption", func(t *testing.T) {
		strategy := compaction.Strategy(nil)
		_, err := store.Open(fake.ExistingDir(), strategy)
		assert.NoError(t, err)
	})

	t.Run("should apply all options", func(t *testing.T) {
		var option1Applied, option2Applied bool
		strategy := compaction.Strategy(
			func(*compaction.Compacter) error {
				option1Applied = true
				return nil
			},
			func(*compaction.Compacter) error {
				option2Applied = true
				return nil
			},
		)
		// when
		openStore(t, strategy)
		// then
		assert.True(t, option1Applied)
		assert.True(t, option2Applied)
	})

	t.Run("should eventually remove all files", func(t *testing.T) {
		s := openStore(t, compaction.Strategy(compaction.MaxVersions(0), compaction.MinVersions(0)))
		defer s.Close()
		storetest.WriteData(t, s, []byte("data"))
		assert.Eventually(t, allFilesRemoved(s), time.Second, time.Millisecond)

		t.Run("and remove them again after updating state", func(t *testing.T) {
			storetest.WriteData(t, s, []byte("data"))
			assert.Eventually(t, allFilesRemoved(s), time.Second, time.Millisecond)
		})
	})

	t.Run("should remove all files after DeleteFile was failed for the first time", func(t *testing.T) {
		dir := failing.DeleteFileOnce(fake.ExistingDir())
		s := openStoreWithDir(t, dir,
			compaction.Strategy(
				compaction.MaxVersions(0),
				compaction.MinVersions(0),
				compaction.Interval(time.Millisecond),
			))
		defer s.Close()
		storetest.WriteData(t, s, []byte("data"))
		assert.Eventually(t, allFilesRemoved(s), time.Second, time.Millisecond)
	})
}

func TestCompacter(t *testing.T) {
	t.Run("should remove one state when two states were stored and MaxVersions is 0 and MinVersion is 1", func(t *testing.T) {
		compacter := &compaction.Compacter{}
		applyMaxVersions := compaction.MaxVersions(0)
		err := applyMaxVersions(compacter)
		require.NoError(t, err)
		applyMinVersions := compaction.MinVersions(1)
		err = applyMinVersions(compacter)
		require.NoError(t, err)
		state := &fake.State{}
		compacter.Start(context.Background(), state)
		// when
		state.AddVersion(1)
		state.AddVersion(2)
		// then
		assert.Eventually(t, stateRevisionsAre(state, 2), time.Second, time.Millisecond)
	})
}

func allFilesRemoved(s *store.Store) func() bool {
	return func() bool {
		_, err := s.Reader()
		if err == nil {
			return false
		}
		return store.IsDataNotFound(err)
	}
}

func TestMaxVersions(t *testing.T) {
	t.Run("negative max returns error", func(t *testing.T) {
		strategy := compaction.Strategy(compaction.MaxVersions(-1))
		_, err := store.Open(fake.ExistingDir(), strategy)
		assert.Error(t, err)
	})

	t.Run("should remove one state when two states were stored and MaxVersions is 1", func(t *testing.T) {
		compacter := &compaction.Compacter{}
		applyMaxVersions := compaction.MaxVersions(1)
		err := applyMaxVersions(compacter)
		require.NoError(t, err)
		state := &fake.State{}
		compacter.Start(context.Background(), state)
		// when
		state.AddVersion(1)
		state.AddVersion(2)
		// then
		assert.Eventually(t, stateRevisionsAre(state, 2), time.Second, time.Millisecond)
	})

	t.Run("should remove two states when three states were stored and MaxVersions is 2", func(t *testing.T) {
		compacter := &compaction.Compacter{}
		applyMaxVersions := compaction.MaxVersions(2)
		err := applyMaxVersions(compacter)
		require.NoError(t, err)
		state := &fake.State{}
		compacter.Start(context.Background(), state)
		// when
		state.AddVersion(1)
		state.AddVersion(2)
		state.AddVersion(3)
		// then
		assert.Eventually(t, stateRevisionsAre(state, 2, 3), time.Second, time.Millisecond)
	})
}

func stateRevisionsAre(s *fake.State, expected ...int) func() bool {
	return func() bool {
		return assert.ObjectsAreEqual(expected, s.Revisions())
	}
}

func TestMinVersions(t *testing.T) {
	t.Run("negative min returns error", func(t *testing.T) {
		strategy := compaction.Strategy(compaction.MinVersions(-1))
		_, err := store.Open(fake.ExistingDir(), strategy)
		assert.Error(t, err)
	})

	t.Run("should preserve state when number of versions smaller than MinVersions", func(t *testing.T) {
		compacter := &compaction.Compacter{}
		applyMinVersions := compaction.MinVersions(2)
		err := applyMinVersions(compacter)
		require.NoError(t, err)
		state := &fake.State{}
		compacter.Start(context.Background(), state)
		// when
		state.AddVersion(1)
		// then
		assert.Eventually(t, stateRevisionsAre(state, 1), time.Second, time.Millisecond)
	})
}

func TestInterval(t *testing.T) {
	t.Run("negative interval returns error", func(t *testing.T) {
		strategy := compaction.Strategy(compaction.Interval(-1))
		_, err := store.Open(fake.ExistingDir(), strategy)
		assert.Error(t, err)
	})

	t.Run("zero interval returns error", func(t *testing.T) {
		strategy := compaction.Strategy(compaction.Interval(0))
		_, err := store.Open(fake.ExistingDir(), strategy)
		assert.Error(t, err)
	})
}

func openStore(t *testing.T, options ...store.Option) *store.Store {
	return openStoreWithDir(t, fake.ExistingDir(), options...)
}

func openStoreWithDir(t *testing.T, dir store.Dir, options ...store.Option) *store.Store {
	s, err := store.Open(dir, options...)
	require.NoError(t, err)
	return s
}
