package store

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"time"
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
		state: openState(dir),
		now:   time.Now,
	}

	for _, apply := range options {
		if apply != nil {
			if err := apply(db); err != nil {
				return nil, fmt.Errorf("applying option failed: %w", err)
			}
		}
	}

	if err := db.useDefaultDataIntegrityCheckerIfNotSet(); err != nil {
		return nil, err
	}
	db.useDefaultCompacterIfNotSet()
	db.startCompacter()
	return db, nil
}

type Option func(db *DB) error

func IntegrityChecker(checker DataIntegrityChecker) Option {
	return func(db *DB) error {
		return db.setDataIntegrityChecker(checker)
	}
}

func Now(now TimeNow) Option {
	return func(db *DB) error {
		db.now = now
		return nil
	}
}

type TimeNow func() time.Time

type DB struct {
	dir                  Dir
	dataIntegrityChecker DataIntegrityChecker
	compacter            CompactState
	cancelCompacter      context.CancelFunc
	state                *state
	now                  TimeNow
}

type ReadChecksum func(algorithm string) ([]byte, error)
type WriteChecksum func(algorithm string, sum []byte) error

type DataIntegrityChecker interface {
	// Should calculate checksum and compare it with checksum read using readChecksum function on Close
	DecorateReader(reader io.ReadCloser, name string, readChecksum ReadChecksum) io.ReadCloser
	// Should calculate checksum and save it using writeChecksum on Close
	DecorateWriter(writer io.WriteCloser, name string, writeChecksum WriteChecksum) io.WriteCloser
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
	return db.dataIntegrityChecker.DecorateWriter(writer, name, db.writeChecksum(name)), nil
}

func (db *DB) nextVersionFilename(stateDir Dir) (string, error) {
	files, err := stateDir.ListFiles()
	if err != nil {
		return "", err
	}
	filename, exists := filterDataFiles(files).youngestFilename()
	now := db.now()
	if !exists {
		return generateFilename(0, now), nil
	}
	version := filename.version + 1
	return generateFilename(version, now), nil
}

// Returns Reader for last updated version of the state
func (db *DB) Reader() (io.ReadCloser, error) {
	file, err := db.latestIntegralFilename(db.dir)
	if err != nil {
		return nil, err
	}
	reader, err := db.dir.FileReader(file)
	if err != nil {
		return nil, err
	}
	return db.dataIntegrityChecker.DecorateReader(reader, file, db.readChecksum(file)), nil
}

func (db *DB) setDataIntegrityChecker(checker DataIntegrityChecker) error {
	if db.dataIntegrityChecker != nil {
		return fmt.Errorf("DataIntegrityChecker configured twice")
	}
	db.dataIntegrityChecker = checker
	return nil
}

func (db *DB) useDefaultDataIntegrityCheckerIfNotSet() error {
	if db.dataIntegrityChecker != nil {
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

func (db *DB) writeChecksum(name string) WriteChecksum {
	return func(algorithm string, sum []byte) error {
		return writeFile(db.dir, name+"."+algorithm, sum)
	}
}

func writeFile(dir Dir, name string, payload []byte) error {
	writer, err := dir.FileWriter(name)
	if err != nil {
		return err
	}
	_, err = writer.Write(payload)
	if err != nil {
		_ = writer.Close()
		return err
	}
	return writer.Close()
}

func (db *DB) readChecksum(name string) ReadChecksum {
	return func(algorithm string) ([]byte, error) {
		reader, err := db.dir.FileReader(name + "." + algorithm)
		if err != nil {
			return nil, err
		}
		return ioutil.ReadAll(reader)
	}
}

func (db *DB) latestIntegralFilename(dir Dir) (string, error) {
	files, err := dir.ListFiles()
	if err != nil {
		return "", err
	}
	dataFiles := filterDataFiles(files)
	sortByVersionDescending(dataFiles)
	if len(dataFiles) == 0 {
		return "", &dataNotFoundError{}
	}
	for _, dataFile := range dataFiles {
		if err := db.verifyChecksum(dir, dataFile); err == nil {
			return dataFile.name, nil
		}
	}
	return "", &dataNotFoundError{}
}

func (db *DB) verifyChecksum(dir Dir, file filename) error {
	fileReader, err := dir.FileReader(file.name)
	if err != nil {
		return err
	}
	reader := db.dataIntegrityChecker.DecorateReader(fileReader, file.name, db.readChecksum(file.name))
	if err := readAll(reader); err != nil {
		_ = reader.Close()
		return err
	}
	return reader.Close()
}

func readAll(reader io.ReadCloser) error {
	buffer := make([]byte, 4096) // FIXME reuse the buffer and make it configurable
	for {
		_, err := reader.Read(buffer)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}
