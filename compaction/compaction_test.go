package compaction_test

import (
	"errors"
	"testing"

	"github.com/jacekolszak/deebee/compaction"
	"github.com/jacekolszak/deebee/fake"
	"github.com/jacekolszak/deebee/store"
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
		_, err := store.Open(fake.ExistingDir(), strategy)
		// then
		require.NoError(t, err)
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
		_, err := store.Open(fake.ExistingDir(), strategy)
		// then
		require.NoError(t, err)
		assert.True(t, option1Applied)
		assert.True(t, option2Applied)
	})
}
