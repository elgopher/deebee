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
	dirExists, err := dir.Exists()
	if err != nil {
		return nil, err
	}
	if !dirExists {
		return nil, newClientError(fmt.Sprintf("database dir %s not found", dir))
	}

	s := &DB{
		dir: dir,
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

type Option func(db *DB) error

// DB stores states. Each state has a key and data.
type DB struct {
	dir Dir
}

// Returns Writer for new version of state with given key
func (db *DB) Writer(key string) (io.WriteCloser, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}

	stateDir := db.dir.Dir(key)
	stateDirExists, err := stateDir.Exists()
	if err != nil {
		return nil, err
	}
	if !stateDirExists {
		if err := stateDir.Mkdir(); err != nil {
			return nil, err
		}
	}
	name, err := db.nextVersionFilename(stateDir)
	if err != nil {
		return nil, err
	}
	return stateDir.FileWriter(name)
}

func (db *DB) nextVersionFilename(stateDir Dir) (string, error) {
	files, err := stateDir.ListFiles()
	if err != nil {
		return "", err
	}
	filename, exists := youngestFilename(toFilenames(files))
	if !exists {
		return "0", nil
	}
	version := filename.version + 1
	name := generateFilename(version)
	return name, nil
}

// Returns Reader for state with given key
func (db *DB) Reader(key string) (io.ReadCloser, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}

	stateDir := db.dir.Dir(key)
	stateDirExists, err := stateDir.Exists()
	if err != nil {
		return nil, err
	}
	if !stateDirExists {
		return nil, &dataNotFoundError{}
	}
	files, err := stateDir.ListFiles()
	if err != nil {
		return nil, err
	}
	dataFile, exists := youngestFilename(toFilenames(files))
	if !exists {
		return nil, &dataNotFoundError{}
	}
	return stateDir.FileReader(dataFile.name)
}

// Dir is a filesystem abstraction useful for unit testing and decoupling the code from `os` package.
//
// Names with file separators are not supported
type Dir interface {
	// Opens an existing file for read. Must return error when file does not exist
	FileReader(name string) (io.ReadCloser, error)
	// Creates a new file for write. Must return error when file already exists
	FileWriter(name string) (FileWriter, error)
	// Creates this directory. Do nothing when directory already exists
	Mkdir() error
	// Return directory with name. Does not check immediately if dir exists.
	Dir(name string) Dir
	// Returns true when directory exists
	Exists() (bool, error)
	// List files excluding directories
	ListFiles() ([]string, error)
}

type FileWriter interface {
	io.WriteCloser
	Sync() error
}
