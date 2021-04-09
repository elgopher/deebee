// (c) 2021 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

package compacter

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/jacekolszak/deebee/codec"
	"github.com/jacekolszak/deebee/store"
)

func RunOnce(s Store, options ...Option) error {
	if s == nil {
		return errors.New("nil store")
	}

	_, err := applyOptions(options)
	if err != nil {
		return err
	}

	versions, err := s.Versions()
	if err != nil {
		return fmt.Errorf("error getting versions: %w", err)
	}

	if len(versions) > 1 {
		latestVersion, err := codec.ReadLatest(s, readAllDiscarding)
		if err != nil {
			return fmt.Errorf("error getting latest integral version: %w", err)
		}
		for _, v := range versions {
			if v == latestVersion {
				return nil
			}
			if err := s.DeleteVersion(v.Time); err != nil {
				return fmt.Errorf("error when deleting version: %w", err)
			}
		}
	}

	return nil
}

func Start(ctx context.Context, s Store, options ...Option) error {
	if s == nil {
		return errors.New("nil store")
	}

	opts, err := applyOptions(options)
	if err != nil {
		return err
	}

	for {
		select {
		case <-time.After(opts.interval):
			if err := RunOnce(s, options...); err != nil {
				log.Printf("compacter.RunOnce failed: %s", err)
			}
		case <-ctx.Done():
			return nil
		}
	}
}

type Store interface {
	Reader(...store.ReaderOption) (store.Reader, error)
	Versions() ([]store.Version, error)
	DeleteVersion(time.Time) error
}

type Option func(options *Options) error

type Options struct {
	interval time.Duration
}

func Interval(d time.Duration) Option {
	return func(options *Options) error {
		options.interval = d
		return nil
	}
}

func applyOptions(options []Option) (*Options, error) {
	opts := &Options{
		interval: time.Minute,
	}

	for _, apply := range options {
		if apply == nil {
			continue
		}
		if err := apply(opts); err != nil {
			return nil, fmt.Errorf("error applying option: %w", err)
		}
	}
	return opts, nil
}

func readAllDiscarding(reader io.Reader) error {
	block := make([]byte, 512)
	for {
		_, err := reader.Read(block)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}
