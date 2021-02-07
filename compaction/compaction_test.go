package compaction_test

import (
	"errors"
	"testing"
	"time"

	"github.com/jacekolszak/deebee/compaction"
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
		allFilesRemoved := func() bool {
			_, err := s.Reader()
			if err == nil {
				return false
			}
			return store.IsDataNotFound(err)
		}
		assert.Eventually(t, allFilesRemoved, time.Second, time.Millisecond)

		t.Run("and remove them again after updating state", func(t *testing.T) {
			storetest.WriteData(t, s, []byte("data"))
			assert.Eventually(t, allFilesRemoved, time.Second, time.Millisecond)
		})
	})
}

func TestMaxVersions(t *testing.T) {
	t.Run("negative max returns error", func(t *testing.T) {
		strategy := compaction.Strategy(compaction.MaxVersions(-1))
		_, err := store.Open(fake.ExistingDir(), strategy)
		assert.Error(t, err)
	})
}

func TestMinVersions(t *testing.T) {
	t.Run("negative min returns error", func(t *testing.T) {
		strategy := compaction.Strategy(compaction.MinVersions(-1))
		_, err := store.Open(fake.ExistingDir(), strategy)
		assert.Error(t, err)
	})
}

func openStore(t *testing.T, options ...store.Option) *store.Store {
	s, err := store.Open(fake.ExistingDir(), options...)
	require.NoError(t, err)
	return s
}