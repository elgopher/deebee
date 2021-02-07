package deebee

import (
	"context"

	"github.com/jacekolszak/deebee/checksum"
	"github.com/jacekolszak/deebee/store"
)

func Open(dir store.Dir, options ...store.Option) (*store.DB, error) {
	defaultOptions := []store.Option{
		checksum.IntegrityChecker(),
		store.Compacter(noCompact),
	}
	mergedOptions := append(defaultOptions, options...)
	return store.Open(dir, mergedOptions...)
}

func noCompact(context.Context, store.State) {}
