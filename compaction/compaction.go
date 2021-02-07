package compaction

import (
	"context"
	"fmt"
	"time"

	"github.com/jacekolszak/deebee/store"
)

func Strategy(options ...StrategyOption) store.Option {
	return func(s *store.Store) error {
		compacter := &Compacter{
			interval: time.Minute,
		}
		for _, option := range options {
			if option == nil {
				return nil
			}
			if err := option(compacter); err != nil {
				return err
			}
		}
		return store.Compacter(compacter.start)(s)
	}
}

type StrategyOption func(*Compacter) error

type Compacter struct {
	interval time.Duration
}

func (c *Compacter) start(ctx context.Context, state store.State) {
	go func() {
		for {
			select {
			case <-state.Updates():
				c.compact(state)
			case <-time.After(c.interval):
				c.compact(state)
			}
		}
	}()
}

func (c *Compacter) compact(state store.State) {
	versions, _ := state.Versions()
	for _, version := range versions {
		_ = version.Remove()
	}
}

func MaxVersions(max int) StrategyOption {
	return func(compacter *Compacter) error {
		if max < 0 {
			return fmt.Errorf("negative max in compaction.MaxVersions: %d", max)
		}
		return nil
	}
}

func MinVersions(min int) StrategyOption {
	return func(compacter *Compacter) error {
		if min < 0 {
			return fmt.Errorf("negative max in compaction.MinVersions: %d", min)
		}
		return nil
	}
}

func Interval(i time.Duration) StrategyOption {
	return func(compacter *Compacter) error {
		if i < 0 {
			return fmt.Errorf("negative interval in compaction.Interval: %d", i)
		}
		if i == 0 {
			return fmt.Errorf("zero interval in compaction.Interval")
		}
		compacter.interval = i
		return nil
	}
}
