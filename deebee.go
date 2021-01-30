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
		fileIntegrityChecker: &checksumIntegrityChecker{
			newSum:                 newFnv128a,
			latestIntegralFilename: lazyLatestIntegralFilename,
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

type Option func(db *DB) error

// DB stores states. Each state has a key and data.
type DB struct {
	dir                  Dir
	fileIntegrityChecker FileIntegrityChecker
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
	writer, err := stateDir.FileWriter(name)
	if err != nil {
		return nil, err
	}
	return db.fileIntegrityChecker.DecorateWriter(writer, stateDir, name), nil
}

func (db *DB) nextVersionFilename(stateDir Dir) (string, error) {
	files, err := stateDir.ListFiles()
	if err != nil {
		return "", err
	}
	filename, exists := filterDatafiles(files).youngestFilename()
	if !exists {
		return "0", nil
	}
	version := filename.version + 1
	return generateFilename(version), nil
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
	file, err := db.fileIntegrityChecker.LatestIntegralFilename(stateDir)
	if err != nil {
		return nil, err
	}
	reader, err := stateDir.FileReader(file)
	if err != nil {
		return nil, err
	}
	return db.fileIntegrityChecker.DecorateReader(reader, stateDir, file), nil
}
