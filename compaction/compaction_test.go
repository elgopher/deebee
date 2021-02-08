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
	t.Run("should accept strategy with default options", func(t *testing.T) {
		defaultStrategy := compaction.Strategy()
		_, err := store.Open(fake.ExistingDir(), defaultStrategy)
		assert.NoError(t, err)
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

func TestNewCompacter(t *testing.T) {
	t.Run("should return error when StrategyOption returned error", func(t *testing.T) {
		option := func(*compaction.Compacter) error {
			return errors.New("error")
		}
		_, err := compaction.NewCompacter(option)
		assert.Error(t, err)
	})

	t.Run("should create compacter without options", func(t *testing.T) {
		c, err := compaction.NewCompacter()
		assert.NoError(t, err)
		assert.NotNil(t, c)
	})

	t.Run("should pass new Compacter instance to custom StrategyOption", func(t *testing.T) {
		var compactionStrategyReceived *compaction.Compacter
		option := func(s *compaction.Compacter) error {
			compactionStrategyReceived = s
			return nil
		}
		// when
		_, err := compaction.NewCompacter(option)
		require.NoError(t, err)
		// then
		assert.NotNil(t, compactionStrategyReceived)
	})

	t.Run("should skip nil StrategyOption", func(t *testing.T) {
		var secondOptionApplied bool
		secondOption := func(compacter *compaction.Compacter) error {
			secondOptionApplied = true
			return nil
		}
		_, err := compaction.NewCompacter(nil, secondOption)
		assert.NoError(t, err)
		assert.True(t, secondOptionApplied)
	})

	t.Run("should apply all options", func(t *testing.T) {
		var option1Applied, option2Applied bool
		// when
		_, err := compaction.NewCompacter(
			func(*compaction.Compacter) error {
				option1Applied = true
				return nil
			},
			func(*compaction.Compacter) error {
				option2Applied = true
				return nil
			},
		)
		// then
		require.NoError(t, err)
		assert.True(t, option1Applied)
		assert.True(t, option2Applied)
	})
}

func TestCompacter_Start(t *testing.T) {
	t.Run("should remove one state when two states were stored and MaxVersions is 0 and MinVersion is 1", func(t *testing.T) {
		compacter, err := compaction.NewCompacter(compaction.MaxVersions(0), compaction.MinVersions(1))
		require.NoError(t, err)
		state := &fake.State{}
		startCompacterAsynchronously(t, compacter, state)
		// when
		state.AddVersion(1)
		state.AddVersion(2)
		// then
		assert.Eventually(t, stateRevisionsAre(state, 2), time.Second, time.Millisecond)
	})

	t.Run("should preserve 2 last updates by default", func(t *testing.T) {
		compacter, err := compaction.NewCompacter()
		require.NoError(t, err)
		state := &fake.State{}
		startCompacterAsynchronously(t, compacter, state)
		// when
		state.AddVersion(1)
		state.AddVersion(2)
		state.AddVersion(3)
		// then
		assert.Eventually(t, stateRevisionsAre(state, 2, 3), time.Second, time.Millisecond)
	})

	t.Run("should stop once context is cancelled", func(t *testing.T) {
		compacter, err := compaction.NewCompacter()
		require.NoError(t, err)
		state := &fake.State{}
		ctx, cancel := context.WithCancel(context.Background())

		f := runAsync(func() {
			compacter.Start(ctx, state)
		})
		// when
		cancel()
		// then
		f.waitOrFailAfter(t, time.Second)
	})

	t.Run("should stop once updates channel is closed", func(t *testing.T) {
		compacter, err := compaction.NewCompacter()
		require.NoError(t, err)
		state := &fake.State{}

		f := runAsync(func() {
			compacter.Start(context.Background(), state)
		})
		// when
		state.Close()
		// then
		f.waitOrFailAfter(t, time.Second)
	})
}

func startCompacterAsynchronously(t *testing.T, compacter *compaction.Compacter, state *fake.State) {
	ctx, cancel := context.WithCancel(context.Background())
	go compacter.Start(ctx, state)
	t.Cleanup(cancel)
}

func runAsync(f func()) async {
	done := make(chan struct{})
	go func() {
		f()
		close(done)
	}()
	return async{done: done}
}

type async struct {
	done <-chan struct{}
}

func (a *async) waitOrFailAfter(t *testing.T, timeout time.Duration) {
	select {
	case <-a.done:
	case <-time.After(timeout):
		assert.FailNow(t, "timeout waiting for async function to finish")
	}
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
		_, err := compaction.NewCompacter(compaction.MaxVersions(-1))
		assert.Error(t, err)
	})

	t.Run("should remove one state when two states were stored and MaxVersions is 1", func(t *testing.T) {
		compacter, err := compaction.NewCompacter(compaction.MaxVersions(1))
		require.NoError(t, err)
		state := &fake.State{}
		startCompacterAsynchronously(t, compacter, state)
		// when
		state.AddVersion(1)
		state.AddVersion(2)
		// then
		assert.Eventually(t, stateRevisionsAre(state, 2), time.Second, time.Millisecond)
	})

	t.Run("should remove two states when three states were stored and MaxVersions is 2", func(t *testing.T) {
		compacter, err := compaction.NewCompacter(compaction.MaxVersions(2))
		require.NoError(t, err)
		state := &fake.State{}
		startCompacterAsynchronously(t, compacter, state)
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
		_, err := compaction.NewCompacter(compaction.MinVersions(-1))
		assert.Error(t, err)
	})

	t.Run("should preserve state when number of versions smaller than MinVersions", func(t *testing.T) {
		compacter, err := compaction.NewCompacter(compaction.MinVersions(2))
		require.NoError(t, err)
		state := &fake.State{}
		startCompacterAsynchronously(t, compacter, state)
		// when
		state.AddVersion(1)
		// then
		assert.Eventually(t, stateRevisionsAre(state, 1), time.Second, time.Millisecond)
	})
}

func TestInterval(t *testing.T) {
	t.Run("negative interval returns error", func(t *testing.T) {
		_, err := compaction.NewCompacter(compaction.Interval(-1))
		assert.Error(t, err)
	})

	t.Run("zero interval returns error", func(t *testing.T) {
		_, err := compaction.NewCompacter(compaction.Interval(0))
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
