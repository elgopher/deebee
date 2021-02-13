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
		s.dataIntegrityChecker = checker
		return nil
	}
}

func NoDataIntegrityCheck() Option {
	return func(s *Store) error {
		s.dataIntegrityChecker = noDataIntegrityCheck{}
		return nil
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
type ReadChecksum func() ([]byte, error)

type WriteChecksum func(sum []byte) error

type DataIntegrityChecker interface {
	// Should calculate checksum and compare it with checksum read using readChecksum function on Close
	DecorateReader(reader io.ReadCloser, readChecksum ReadChecksum) (io.ReadCloser, error)
	// Should calculate checksum and save it using writeChecksum on Close
	DecorateWriter(writer io.WriteCloser, writeChecksum WriteChecksum) (io.WriteCloser, error)
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
	return s.dataIntegrityChecker.DecorateWriter(writer, s.writeChecksum(name))
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
	return s.dataIntegrityChecker.DecorateReader(reader, s.readChecksum(file))
}

func (s *Store) useDefaultDataIntegrityCheckerIfNotSet() error {
	if s.dataIntegrityChecker != nil {
		return nil
	}
	s.dataIntegrityChecker = noDataIntegrityCheck{}
	return nil
}

type noDataIntegrityCheck struct{}

func (n noDataIntegrityCheck) DecorateReader(reader io.ReadCloser, _ ReadChecksum) (io.ReadCloser, error) {
	return reader, nil
}

func (n noDataIntegrityCheck) DecorateWriter(writer io.WriteCloser, _ WriteChecksum) (io.WriteCloser, error) {
	return writer, nil
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
	return func(sum []byte) error {
		return writeFile(s.dir, checksumFilename(name), sum)
	}
}

func checksumFilename(name string) string {
	return name + ".checksum"
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
	return func() ([]byte, error) {
		reader, err := s.dir.FileReader(checksumFilename(name))
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
	reader, err := s.dataIntegrityChecker.DecorateReader(fileReader, s.readChecksum(file.name))
	if err != nil {
		return err
	}
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
