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
			chooseForRemoval: func(versions []store.StateVersion) []store.StateVersion {
				return versions
			},
		}
		for _, option := range options {
			if option == nil {
				return nil
			}
			if err := option(compacter); err != nil {
				return err
			}
		}
		return store.Compacter(compacter.Start)(s)
	}
}

type StrategyOption func(*Compacter) error

type Compacter struct {
	interval         time.Duration
	chooseForRemoval func([]store.StateVersion) []store.StateVersion
	minVersions      int
}

func (c *Compacter) Start(ctx context.Context, state store.State) {
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
	versions = c.excludeMinimal(versions)
	forRemoval := c.chooseForRemoval(versions)
	for _, v := range forRemoval {
		_ = v.Remove()
	}
}

func (c *Compacter) excludeMinimal(versions []store.StateVersion) []store.StateVersion {
	if len(versions) < c.minVersions {
		return nil
	}
	return versions[:len(versions)-c.minVersions]
}

func MaxVersions(max int) StrategyOption {
	return func(compacter *Compacter) error {
		if max < 0 {
			return fmt.Errorf("negative max in compaction.MaxVersions: %d", max)
		}
		compacter.chooseForRemoval = func(versions []store.StateVersion) []store.StateVersion {
			if len(versions) <= max {
				return nil
			}
			return versions[:len(versions)-max]
		}
		return nil
	}
}

func MinVersions(min int) StrategyOption {
	return func(compacter *Compacter) error {
		if min < 0 {
			return fmt.Errorf("negative max in compaction.MinVersions: %d", min)
		}
		compacter.minVersions = min
		if compacter.chooseForRemoval == nil {
			compacter.chooseForRemoval = func(versions []store.StateVersion) []store.StateVersion {
				return nil
			}
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
