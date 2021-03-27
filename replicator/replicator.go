// (c) 2021 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

// Package replicator provides very simple replication functionality which copies most recent state version from one store
// to another. This can be used for example when you want to replicate state from local disk to a shared (network) disk
// such as AWS EFS.
package replicator

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/jacekolszak/deebee/store"
)

func CopyFromTo(from ReadOnlyStore, to WriteOnlyStore) error {
	versions, _ := from.Versions(store.NewestFirst, store.Limit(1))
	if len(versions) == 0 {
		return errors.New("no versions available")
	}
	version := versions[0]
	return copyVersion(version, from, to)
}

// Replicate state asynchronously in one minute intervals
func StartFromTo(ctx context.Context, from ReadOnlyStore, to WriteOnlyStore, options ...Option) {
	CopyFromTo(from, to)
}

type ReadOnlyStore interface {
	Reader(...store.ReaderOption) (store.Reader, error)
	Versions(...store.VersionsOption) ([]store.Version, error)
}

type WriteOnlyStore interface {
	Writer(options ...store.WriterOption) (store.Writer, error)
}

type Option func() error

func Interval(duration time.Duration) Option {
	return func() error {
		return nil
	}
}

func copyVersion(version store.Version, from ReadOnlyStore, to WriteOnlyStore) error {
	reader, err := from.Reader(store.Time(version.Time)) // Reader for specific version does not check integrity before
	if err != nil {
		return err
	}
	// explicitly set version - will fail if file already exists
	writer, err := to.Writer(store.WriteTime(version.Time))
	if err != nil {
		return err
	}
	_, err = io.Copy(writer, reader)
	if err != nil {
		writer.AbortAndClose()
		return err
	}
	if err := reader.Close(); err != nil {
		writer.AbortAndClose()
		return err
	}
	return writer.Close()
}
