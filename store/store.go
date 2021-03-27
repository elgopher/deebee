// (c) 2021 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

package store

import (
	"io"
	"time"
)

func Open(dir string, options ...Option) (*Store, error) {
	return &Store{}, nil
}

type Option func(s *Store) error

type Store struct {
}

func (s *Store) Reader(options ...ReaderOption) (Reader, error) {
	return nil, nil
}

type ReaderOption func() error

func Time(time.Time) ReaderOption {
	return func() error {
		return nil
	}
}

type Reader interface {
	io.ReadCloser
	Version() Version
}

func (s *Store) Writer(options ...WriterOption) (Writer, error) {
	return nil, nil
}

type WriterOption func() error

// WriteTime is not named Time to avoid name conflict with ReaderOption
func WriteTime(time.Time) WriterOption {
	return func() error {
		return nil
	}
}

var NoSync WriterOption = func() error {
	return nil
}

type Writer interface {
	io.Writer
	// Close must be called to make state readable
	Close() error
	// Version size increases when writing?
	Version() Version
	// AbortAndClose aborts writing version. Version will not be available to read.
	AbortAndClose()
}

func (s *Store) Versions(options ...VersionsOption) ([]Version, error) {
	panic("implement me")
}

type VersionsOption func() error

var OldestFirst VersionsOption = func() error {
	return nil
}
var NewestFirst VersionsOption = func() error {
	return nil
}

func Limit(max int) VersionsOption {
	return func() error {
		return nil
	}
}

type Version struct {
	// Time uniquely identifies version
	Time time.Time
	Size int
}

func (s *Store) DeleteVersion(time.Time) error {
	panic("implement me")
}

func (s *Store) Metrics() Metrics {
	return Metrics{}
}
