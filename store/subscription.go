package store

import "sync"

type subscriptions struct {
	active []*Subscription
	mutex  sync.Mutex
}

func (s *subscriptions) notify() {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	for _, subscription := range s.active {
		subscription.notifyUpdated()
	}
}

func (s *subscriptions) newSubscription() *Subscription {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	subscription := &Subscription{
		updates: make(chan struct{}, 1),
	}
	subscription.notifyClosed = func() {
		s.removeSubscription(subscription)
	}
	s.active = append(s.active, subscription)
	return subscription
}

func (s *subscriptions) removeSubscription(subscription *Subscription) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	for i := 0; i < len(s.active); i++ {
		if s.active[i] == subscription {
			s.active = append(s.active[:i], s.active[i+1:]...)
		}
	}
}

func (s *subscriptions) close() {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	for _, subscription := range s.active {
		close(subscription.updates)
	}
	s.active = nil
}

func (s *Store) SubscribeUpdates() *Subscription {
	return s.subscriptions.newSubscription()
}

type Subscription struct {
	updates      chan struct{}
	closed       bool
	notifyClosed func()
}

// Updates returns channel informing subscriber that state was updated. If subscriber can't keep up, then some updates
// might be discarded.
func (s *Subscription) Updates() <-chan struct{} {
	return s.updates
}

func (s *Subscription) Close() {
	s.notifyClosed()
	if s.closed {
		return
	}
	s.closed = true
	close(s.updates)
}

func (s *Subscription) notifyUpdated() {
	if s.closed {
		return
	}
	select {
	case s.updates <- struct{}{}:
	default:
	}
}
