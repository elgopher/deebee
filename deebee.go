package deebee

import (
	"errors"
	"fmt"
	"io"
)

func Open(dir Dir, options ...Option) (*DB, error) {
	if dir == nil {
		return nil, errors.New("nil dir")
	}
	s := &DB{
		dir: dir,
		newChecksum: func() Checksum {
			return &zeroChecksum{}
		},
	}
	for _, apply := range options {
		if apply != nil {
			if err := apply(s); err != nil {
				return nil, fmt.Errorf("applying option failed: %w", err)
			}
		}
	}
	return s, nil
}

type Option func(state *DB) error

type DB struct {
	dir         Dir
	newChecksum NewChecksum
}

func (s *DB) NewWriter(key string) (io.WriteCloser, error) {
	openWriter, err := openWriterFunc(s.dir, s.newChecksum)
	if err != nil {
		return nil, err
	}
	return openWriter(key)
}

func (s *DB) NewReader(key string) (io.ReadCloser, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}
	openReader, err := openReaderFunc(s.dir, s.newChecksum)
	if err != nil {
		return nil, err
	}
	return openReader(key)
}

func WithNewChecksum(newChecksum NewChecksum) Option {
	return func(state *DB) error {
		state.newChecksum = newChecksum
		return nil
	}
}
