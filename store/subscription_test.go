package store_test

import (
	"testing"

	"github.com/jacekolszak/deebee/fake"
	"github.com/jacekolszak/deebee/internal/storetest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStore_SubscribeUpdates(t *testing.T) {
	t.Run("should return subscription", func(t *testing.T) {
		store := openStore(t, fake.ExistingDir())
		subscription := store.SubscribeUpdates()
		assert.NotNil(t, subscription)
	})

	t.Run("each call returns a new subscription", func(t *testing.T) {
		store := openStore(t, fake.ExistingDir())
		assert.NotSame(t, store.SubscribeUpdates(), store.SubscribeUpdates())
	})

	t.Run("should close subscription", func(t *testing.T) {
		store := openStore(t, fake.ExistingDir())
		subscription := store.SubscribeUpdates()
		// when
		subscription.Close()
	})
}

func TestSubscription_Updates(t *testing.T) {
	t.Run("should notify about write", func(t *testing.T) {
		s := openStore(t, fake.ExistingDir())
		subscription := s.SubscribeUpdates()
		updates := subscription.Updates()
		// when
		storetest.WriteData(t, s, []byte("data"))
		// then
		assertUpdateReceived(t, updates)
	})

	t.Run("should work even though the subscriber can't keep up", func(t *testing.T) {
		s := openStore(t, fake.ExistingDir())
		_ = s.SubscribeUpdates()
		for i := 0; i < 100; i++ {
			storetest.WriteData(t, s, []byte("data"))
		}
	})
}

func TestSubscription_Close(t *testing.T) {
	t.Run("should close channel", func(t *testing.T) {
		s := openStore(t, fake.ExistingDir())
		subscription := s.SubscribeUpdates()
		// when
		subscription.Close()
		// then
		assertClosed(t, subscription.Updates())
	})

	t.Run("second close does nothing", func(t *testing.T) {
		s := openStore(t, fake.ExistingDir())
		subscription := s.SubscribeUpdates()
		subscription.Close()
		// when
		subscription.Close()
	})

	t.Run("should write after subscription is closed", func(t *testing.T) {
		s := openStore(t, fake.ExistingDir())
		subscription := s.SubscribeUpdates()
		// when
		subscription.Close()
		// then
		storetest.WriteData(t, s, []byte("data"))
	})
}

func TestStore_Close(t *testing.T) {
	t.Run("should close the subscription channel", func(t *testing.T) {
		s := openStore(t, fake.ExistingDir())
		subscription := s.SubscribeUpdates()
		// when
		err := s.Close()
		// then
		require.NoError(t, err)
		assertClosed(t, subscription.Updates())
	})
}

func TestSubscription_ThreadSafety(t *testing.T) {
	t.Run("test with --race flag should not report data races", func(t *testing.T) {
		s := openStore(t, fake.ExistingDir())

		for i := 0; i < 100; i++ {
			go func() {
				subscription := s.SubscribeUpdates()
				writer, err := s.Writer()
				if err == nil {
					_, _ = writer.Write([]byte("data"))
					writer.Close()
				}
				subscription.Close()
			}()
		}
	})
}
