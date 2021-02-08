package fake

import (
	"sync"
	"time"

	"github.com/jacekolszak/deebee/store"
)

type State struct {
	mutex sync.Mutex

	updates  chan struct{}
	versions []*StateVersion
	closed   bool
}

func (s *State) Updates() <-chan struct{} {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.updatesChannel()
}

func (s *State) updatesChannel() chan struct{} {
	if s.updates == nil {
		s.updates = make(chan struct{}, 1)
	}
	return s.updates
}

func (s *State) Versions() ([]store.StateVersion, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	var v []store.StateVersion
	for _, version := range s.versions {
		v = append(v, version)
	}
	return v, nil
}

func (s *State) AddVersion(rev int) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.closed {
		panic("state closed")
	}

	version := &StateVersion{revision: rev}
	version.remove = func() {
		s.mutex.Lock()
		defer s.mutex.Unlock()

		s.versions = remove(s.versions, version)
	}
	s.versions = append(s.versions, version)

	s.notify()
}

func (s *State) notify() {
	select {
	case s.updatesChannel() <- struct{}{}:
	default:
	}
}

func remove(versions []*StateVersion, version *StateVersion) []*StateVersion {
	for i, v := range versions {
		if v.revision == version.revision {
			return append(versions[0:i], versions[i+1:]...)
		}
	}
	return versions
}

func (s *State) Revisions() []int {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	var revisions []int
	for _, version := range s.versions {
		revisions = append(revisions, version.revision)
	}
	return revisions
}

func (s *State) Close() {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.closed {
		return
	}
	close(s.updatesChannel())
	s.closed = true
}

type StateVersion struct {
	revision int
	remove   func()
}

func (s *StateVersion) Revision() int {
	return s.revision
}

func (s *StateVersion) Remove() error {
	s.remove()
	return nil
}

func (s *StateVersion) Time() time.Time {
	panic("implement me")
}
