package deebee

import (
	"context"
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

	db := &DB{
		dir:   dir,
		state: openState(),
	}

	for _, apply := range options {
		if apply != nil {
			if err := apply(db); err != nil {
				return nil, fmt.Errorf("applying option failed: %w", err)
			}
		}
	}

	if err := db.useDefaultFileIntegrityCheckerIfNotSet(); err != nil {
		return nil, err
	}
	db.useDefaultCompacterIfNotSet()
	db.startCompacter()
	return db, nil
}

type Option func(db *DB) error

func IntegrityChecker(checker FileIntegrityChecker) Option {
	return func(db *DB) error {
		return db.setFileIntegrityChecker(checker)
	}
}

// DB stores states. Each state has a key and data.
type DB struct {
	dir                  Dir
	fileIntegrityChecker FileIntegrityChecker
	compacter            CompactState
	cancelCompacter      context.CancelFunc
	state                *state
}

type FileIntegrityChecker interface {
	LatestIntegralFilename(dir Dir) (string, error)
	// Should return error on Close when checksum verification failed
	DecorateReader(reader io.ReadCloser, dir Dir, name string) io.ReadCloser
	// Should store checksum somewhere on Close.
	DecorateWriter(writer io.WriteCloser, dir Dir, name string) io.WriteCloser
}

// Returns Writer for new version of state
func (db *DB) Writer() (io.WriteCloser, error) {
	defer db.state.notifyUpdated()
	name, err := db.nextVersionFilename(db.dir)
	if err != nil {
		return nil, err
	}
	writer, err := db.dir.FileWriter(name)
	if err != nil {
		return nil, err
	}
	return db.fileIntegrityChecker.DecorateWriter(writer, db.dir, name), nil
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

// Returns Reader for last updated version of the state
func (db *DB) Reader() (io.ReadCloser, error) {
	file, err := db.fileIntegrityChecker.LatestIntegralFilename(db.dir)
	if err != nil {
		return nil, err
	}
	reader, err := db.dir.FileReader(file)
	if err != nil {
		return nil, err
	}
	return db.fileIntegrityChecker.DecorateReader(reader, db.dir, file), nil
}

func (db *DB) setFileIntegrityChecker(checker FileIntegrityChecker) error {
	if db.fileIntegrityChecker != nil {
		return fmt.Errorf("FileIntegrityChecker configured twice")
	}
	db.fileIntegrityChecker = checker
	return nil
}

func (db *DB) useDefaultFileIntegrityCheckerIfNotSet() error {
	if db.fileIntegrityChecker != nil {
		return nil
	}
	return ChecksumIntegrityChecker()(db)
}

func (db *DB) useDefaultCompacterIfNotSet() {
	if db.compacter == nil {
		db.compacter = noCompact
	}
}

func (db *DB) startCompacter() {
	ctx, cancelFunc := context.WithCancel(context.Background())
	db.cancelCompacter = cancelFunc
	db.compacter(ctx, db.state)
}

func (db *DB) Close() error {
	db.cancelCompacter()
	db.state.close()
	return nil
}
