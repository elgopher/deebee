package store

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"time"
)

func Open(dir Dir, options ...Option) (*Store, error) {
	if dir == nil {
		return nil, errors.New("nil dir")
	}
	dirExists, err := dir.Exists()
	if err != nil {
		return nil, err
	}
	if !dirExists {
		return nil, newClientError(fmt.Sprintf("store dir %s not found", dir))
	}

	newStore := &Store{
		dir:   dir,
		state: openState(dir),
		now:   time.Now,
	}

	for _, apply := range options {
		if apply != nil {
			if err := apply(newStore); err != nil {
				return nil, fmt.Errorf("applying option failed: %w", err)
			}
		}
	}

	if err := newStore.useDefaultDataIntegrityCheckerIfNotSet(); err != nil {
		return nil, err
	}
	newStore.useDefaultCompacterIfNotSet()
	newStore.startCompacter()
	return newStore, nil
}

type Option func(*Store) error

func IntegrityChecker(checker DataIntegrityChecker) Option {
	return func(s *Store) error {
		return s.setDataIntegrityChecker(checker)
	}
}

func Now(now TimeNow) Option {
	return func(s *Store) error {
		s.now = now
		return nil
	}
}

type TimeNow func() time.Time

type Store struct {
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
func (s *Store) Writer() (io.WriteCloser, error) {
	defer s.state.notifyUpdated()
	name, err := s.nextVersionFilename(s.dir)
	if err != nil {
		return nil, err
	}
	writer, err := s.dir.FileWriter(name)
	if err != nil {
		return nil, err
	}
	return s.dataIntegrityChecker.DecorateWriter(writer, name, s.writeChecksum(name)), nil
}

func (s *Store) nextVersionFilename(stateDir Dir) (string, error) {
	files, err := stateDir.ListFiles()
	if err != nil {
		return "", err
	}
	filename, exists := filterDataFiles(files).youngestFilename()
	now := s.now()
	if !exists {
		return generateFilename(0, now), nil
	}
	version := filename.version + 1
	return generateFilename(version, now), nil
}

// Returns Reader for last updated version of the state
func (s *Store) Reader() (io.ReadCloser, error) {
	file, err := s.latestIntegralFilename(s.dir)
	if err != nil {
		return nil, err
	}
	reader, err := s.dir.FileReader(file)
	if err != nil {
		return nil, err
	}
	return s.dataIntegrityChecker.DecorateReader(reader, file, s.readChecksum(file)), nil
}

func (s *Store) setDataIntegrityChecker(checker DataIntegrityChecker) error {
	s.dataIntegrityChecker = checker
	return nil
}

func (s *Store) useDefaultDataIntegrityCheckerIfNotSet() error {
	if s.dataIntegrityChecker != nil {
		return nil
	}
	s.dataIntegrityChecker = noDataIntegrityCheck{}
	return nil
}

type noDataIntegrityCheck struct{}

func (n noDataIntegrityCheck) DecorateReader(reader io.ReadCloser, name string, readChecksum ReadChecksum) io.ReadCloser {
	return reader
}

func (n noDataIntegrityCheck) DecorateWriter(writer io.WriteCloser, name string, writeChecksum WriteChecksum) io.WriteCloser {
	return writer
}

func (s *Store) useDefaultCompacterIfNotSet() {
	if s.compacter == nil {
		s.compacter = noCompact
	}
}

func (s *Store) startCompacter() {
	ctx, cancelFunc := context.WithCancel(context.Background())
	s.cancelCompacter = cancelFunc
	s.compacter(ctx, s.state)
}

func (s *Store) Close() error {
	s.cancelCompacter()
	s.state.close()
	return nil
}

func (s *Store) writeChecksum(name string) WriteChecksum {
	return func(algorithm string, sum []byte) error {
		return writeFile(s.dir, name+"."+algorithm, sum)
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

func (s *Store) readChecksum(name string) ReadChecksum {
	return func(algorithm string) ([]byte, error) {
		reader, err := s.dir.FileReader(name + "." + algorithm)
		if err != nil {
			return nil, err
		}
		return ioutil.ReadAll(reader)
	}
}

func (s *Store) latestIntegralFilename(dir Dir) (string, error) {
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
		if err := s.verifyChecksum(dir, dataFile); err == nil {
			return dataFile.name, nil
		}
	}
	return "", &dataNotFoundError{}
}

func (s *Store) verifyChecksum(dir Dir, file filename) error {
	fileReader, err := dir.FileReader(file.name)
	if err != nil {
		return err
	}
	reader := s.dataIntegrityChecker.DecorateReader(fileReader, file.name, s.readChecksum(file.name))
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