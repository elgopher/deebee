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
}

type state struct {
	updates chan struct{}
}

func openState() *state {
	return &state{updates: make(chan struct{}, 1)}
}

func (s *state) close() {
	close(s.updates)
}

func (s *state) Updates() <-chan struct{} {
	return s.updates
}

func (s *state) notifyUpdated() {
	select {
	case s.updates <- struct{}{}:
	default:
	}
}

func noCompact(context.Context, State) {}
