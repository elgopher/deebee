package compaction

import (
	"context"
	"fmt"

	"github.com/jacekolszak/deebee/store"
)

func Strategy(options ...StrategyOption) store.Option {
	return func(s *store.Store) error {
		compacter := &Compacter{}
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

type Compacter struct{}

func (c *Compacter) start(ctx context.Context, state store.State) {
	go func() {
		for {
			select {
			case <-state.Updates():
				versions, _ := state.Versions()
				for _, version := range versions {
					_ = version.Remove()
				}
			}
		}
	}()
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
			return fmt.Errorf("negative max in compaction.MaxVersions: %d", min)
		}
		return nil
	}
}
