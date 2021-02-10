package deebee

import (
	"github.com/jacekolszak/deebee/checksum"
	"github.com/jacekolszak/deebee/compaction"
	"github.com/jacekolszak/deebee/store"
)

func Open(dir store.Dir, options ...store.Option) (*store.Store, error) {
	defaultOptions := []store.Option{
		checksum.IntegrityChecker(),
		compaction.Strategy(compaction.MaxVersions(2)),
	}
	mergedOptions := append(defaultOptions, options...)
	return store.Open(dir, mergedOptions...)
}
