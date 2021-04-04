// (c) 2021 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

// Package replicator provides very simple replication functionality which copies most recent state version from one store
// to another. This can be used for example when you want to replicate state from local disk to a shared (network) disk
// such as AWS EFS.
package replicator

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/jacekolszak/deebee/store"
)

func CopyFromTo(from ReadOnlyStore, to WriteOnlyStore) error {
	if from == nil {
		return errors.New("nil <from> store")
	}
	if to == nil {
		return errors.New("nil <to> store")
	}
	return copyLatest(from, to)
}

// Replicate state asynchronously in one minute intervals
func StartFromTo(ctx context.Context, from ReadOnlyStore, to WriteOnlyStore, options ...Option) error {
	if from == nil {
		return errors.New("nil <from> store")
	}
	if to == nil {
		return errors.New("nil <to> store")
	}

	opts := &Options{
		interval: time.Minute,
	}
	for _, apply := range options {
		if apply == nil {
			continue
		}
		if err := apply(opts); err != nil {
			return fmt.Errorf("error applying option: %w", err)
		}
	}

	for {
		select {
		case <-time.After(opts.interval):
			if err := CopyFromTo(from, to); err != nil {
				log.Printf("replicator.CopyFromTo failed: %s", err)
			}
		case <-ctx.Done():
			return nil
		}
	}
}

type ReadOnlyStore interface {
	Reader(...store.ReaderOption) (store.Reader, error)
	Versions() ([]store.Version, error)
}

type WriteOnlyStore interface {
	Writer(options ...store.WriterOption) (store.Writer, error)
}

type Option func(*Options) error

type Options struct {
	interval time.Duration
}

func Interval(d time.Duration) Option {
	return func(o *Options) error {
		o.interval = d
		return nil
	}
}

func copyLatest(from ReadOnlyStore, to WriteOnlyStore) error {
	reader, err := from.Reader()
	if err != nil {
		return err
	}
	writer, err := to.Writer(store.WriteTime(reader.Version().Time))
	if err != nil {
		_ = reader.Close()
		return err
	}
	_, err = io.Copy(writer, reader)
	if err != nil {
		writer.AbortAndClose()
		_ = reader.Close()
		return err
	}
	if err := reader.Close(); err != nil {
		writer.AbortAndClose()
		return err
	}
	return writer.Close()
}
