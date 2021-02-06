package deebee

import (
	"context"
)

func Compacter(compacter CompactState) Option {
	return func(db *DB) error {
		db.compacter = compacter
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

type StateVersion struct {
	Revision int
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
	dataFiles := filterDatafiles(files)
	sortByVersionAscending(dataFiles)
	var states []StateVersion
	for _, datafile := range dataFiles {
		version := StateVersion{
			Revision: datafile.version,
		}
		states = append(states, version)
	}
	return states, nil
}

func (s *state) notifyUpdated() {
	select {
	case s.updates <- struct{}{}:
	default:
	}
}

func noCompact(context.Context, State) {}
