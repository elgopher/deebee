package store

import (
	"context"
	"fmt"
	"time"
)

func Compacter(compacter CompactState) Option {
	return func(s *Store) error {
		s.compacter = compacter
		return nil
	}
}

type CompactState func(ctx context.Context, state State)

type State interface {
	// Updates returns channel informing Compacter that state was updated. If compacter can't keep up, then some updates
	// might be discarded.
	Updates() <-chan struct{}
	Versions() ([]StateVersion, error)
}

type StateVersion interface {
	Revision() int
	Remove() error
	Time() time.Time
}

type state struct {
	dir     Dir
	updates chan struct{}
}

func openState(dir Dir) *state {
	return &state{
		dir:     dir,
		updates: make(chan struct{}, 1),
	}
}

func (s *state) close() {
	close(s.updates)
}

func (s *state) Updates() <-chan struct{} {
	return s.updates
}

func (s *state) Versions() ([]StateVersion, error) {
	files, err := s.dir.ListFiles()
	if err != nil {
		return nil, err
	}
	dataFiles := filterDataFiles(files)
	sortByVersionAscending(dataFiles)
	var states []StateVersion
	for _, dataFile := range dataFiles {
		version := &stateVersion{
			revision: dataFile.version,
			dataFile: dataFile,
			dir:      s.dir,
		}
		states = append(states, version)
	}
	return states, nil
}

type stateVersion struct {
	revision int
	dataFile filename
	dir      Dir
}

func (s *stateVersion) Remove() error {
	if err := s.dir.DeleteFile(s.dataFile.name); err != nil {
		return fmt.Errorf("deleting data file failed: %w", err)
	}
	if err := s.dir.DeleteFile(checksumFilename(s.dataFile.name)); err != nil {
		return fmt.Errorf("deleting checksum file failed: %w", err)
	}
	return nil
}

func (s *stateVersion) Revision() int {
	return s.revision
}

func (s *stateVersion) Time() time.Time {
	return s.dataFile.time
}

func (s *state) notifyUpdated() {
	select {
	case s.updates <- struct{}{}:
	default:
	}
}

func noCompact(context.Context, State) {}
