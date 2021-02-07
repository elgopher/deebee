package compaction

import (
	"github.com/jacekolszak/deebee/store"
)

func Strategy(options ...StrategyOption) store.Option {
	return func(s *store.Store) error {
		for _, option := range options {
			if option == nil {
				return nil
			}
			if err := option(&Compacter{}); err != nil {
				return err
			}
		}
		return nil
	}
}

type StrategyOption func(*Compacter) error

type Compacter struct{}
