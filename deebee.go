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
	newChecksum := func() Checksum {
		return &zeroChecksum{}
	}
	s := &DB{
		dir:         dir,
		newChecksum: newChecksum,
		openWriter:  openWriterFunc(dir, newChecksum),
		openReader:  openReaderFunc(dir, newChecksum),
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
	openWriter  openWriter
	openReader  openReader
}

func (s *DB) NewWriter(key string) (io.WriteCloser, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}
	return s.openWriter(key)
}

func (s *DB) NewReader(key string) (io.ReadCloser, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}
	return s.openReader(key)
}

func WithNewChecksum(newChecksum NewChecksum) Option {
	return func(state *DB) error {
		state.newChecksum = newChecksum
		return nil
	}
}

type NewChecksum func() Checksum

type Checksum interface {
	Add(b []byte)
	Calculate() uint32
}

// Names with nested directories, files are not supported.
type Dir interface {
	// Opens an existing file for read. Must return error when file does not exist
	FileReader(name string) (io.ReadCloser, error)
	// Creates a new file for write. Must return error when file already exists
	FileWriter(name string) (FileWriter, error)
	// Returns true when directory exists
	DirExists(name string) (bool, error)
	// Creates directory. Do nothing when directory already exists
	Mkdir(name string) error
	// Return directory with name
	Dir(name string) Dir
	// List files excluding directories
	ListFiles() ([]string, error)
}

type FileWriter interface {
	io.WriteCloser
	Sync() error
}
