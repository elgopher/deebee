// (c) 2021 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

package compacter

import (
	"context"
	"time"

	"github.com/jacekolszak/deebee/store"
)

func Start(ctx context.Context, store Store, options ...Option) {

}

func RunOnce(store Store, options ...Option) error {
	return nil
}

type Store interface {
	Reader(...store.ReaderOption) (store.Reader, error)
	Versions(...store.VersionsOption) ([]store.Version, error)
	DeleteVersion(time.Time) error
}

type Option func() error

func MaxVersions(max int) Option {
	return func() error {
		return nil
	}
}
