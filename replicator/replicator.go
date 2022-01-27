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
	"time"

	"github.com/elgopher/deebee/codec"
	"github.com/elgopher/deebee/store"
)

func CopyFromTo(from codec.ReadOnlyStore, to codec.WriteOnlyStore) error {
	if from == nil {
		return errors.New("nil <from> store")
	}
	if to == nil {
		return errors.New("nil <to> store")
	}
	return copyLatest(from, to)
}

// StartFromTo replicates state asynchronously in one minute intervals
func StartFromTo(ctx context.Context, from codec.ReadOnlyStore, to codec.WriteOnlyStore, options ...Option) error {
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
			if err := CopyFromTo(from, to); err != nil && !store.IsVersionAlreadyExists(err) {
				log.WithError(ctx, err).Error("replicator.CopyFromTo failed")
			}
		case <-ctx.Done():
			return nil
		}
	}
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

func copyLatest(from codec.ReadOnlyStore, to codec.WriteOnlyStore) error {
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
