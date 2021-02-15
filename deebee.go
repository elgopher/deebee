// (c) 2021 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

package deebee

import (
	"github.com/jacekolszak/deebee/checksum"
	"github.com/jacekolszak/deebee/compaction"
	"github.com/jacekolszak/deebee/os"
	"github.com/jacekolszak/deebee/store"
)

func Open(dir string, options ...store.Option) (*store.Store, error) {
	defaultOptions := []store.Option{
		checksum.IntegrityChecker(),
		compaction.Strategy(compaction.MaxVersions(2)),
	}
	mergedOptions := append(defaultOptions, options...)
	return store.Open(os.Dir(dir), mergedOptions...)
}
