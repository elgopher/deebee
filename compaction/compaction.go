package compaction

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jacekolszak/deebee/store"
)

func Strategy(options ...StrategyOption) store.Option {
	return func(s *store.Store) error {
		compacter, err := NewCompacter(options...)
		if err != nil {
			return fmt.Errorf("NewCompacter failed: %w", err)
		}
		return store.Compacter(compacter.Start)(s)
	}
}

type StrategyOption func(*Compacter) error

type Compacter struct {
	interval       time.Duration
	keepPolicies   []KeepPolicyFunc
	removePolicies []RemovePolicyFunc
}

func NewCompacter(options ...StrategyOption) (*Compacter, error) {
	compacter := &Compacter{
		interval: time.Minute,
	}
	for _, option := range options {
		if option == nil {
			continue
		}
		if err := option(compacter); err != nil {
			return nil, err
		}
	}
	return compacter, nil
}

func (c *Compacter) Start(ctx context.Context, state store.State) {
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-state.Updates():
			if !ok {
				return
			}
			c.compact(state)
		case <-time.After(c.interval):
			c.compact(state)
		}
	}
}

func (c *Compacter) compact(state store.State) {
	versions, err := state.Versions()
	if err != nil {
		log.Printf("getting state versions failed: %s", err)
		return
	}
	versionsToRemove := c.removalCandidates(versions)
	for _, policy := range c.keepPolicies {
		versionsToKeep := policy(versions)
		for _, v := range versionsToKeep {
			delete(versionsToRemove, v)
		}
	}
	for v := range versionsToRemove {
		if err := v.Remove(); err != nil {
			log.Printf("remove version failed: %s", err)
		}
	}
}

func (c *Compacter) removalCandidates(versions []store.StateVersion) map[store.StateVersion]struct{} {
	removalCandidates := map[store.StateVersion]struct{}{}
	for _, policy := range c.removePolicies {
		candidates := policy(versions)
		for _, candidate := range candidates {
			removalCandidates[candidate] = struct{}{}
		}
	}
	return removalCandidates
}

func MaxVersions(max int) StrategyOption {
	return func(compacter *Compacter) error {
		policy, err := maxVersions(max)
		if err != nil {
			return err
		}
		return RemovePolicy(policy)(compacter)
	}
}

func maxVersions(max int) (RemovePolicyFunc, error) {
	if max < 0 {
		return nil, fmt.Errorf("negative max in compaction.MaxVersions: %d", max)
	}
	return func(versions []store.StateVersion) []store.StateVersion {
		if len(versions) <= max {
			return nil
		}
		return versions[:len(versions)-max]
	}, nil
}

func Interval(i time.Duration) StrategyOption {
	return func(compacter *Compacter) error {
		if i < 0 {
			return fmt.Errorf("negative interval in compaction.Interval: %d", i)
		}
		if i == 0 {
			return fmt.Errorf("zero interval in compaction.Interval")
		}
		compacter.interval = i
		return nil
	}
}

func KeepLatestVersions(min int) StrategyOption {
	return func(compacter *Compacter) error {
		policy, err := keepLatestVersions(min)
		if err != nil {
			return err
		}
		return KeepPolicy(policy)(compacter)
	}
}

func keepLatestVersions(min int) (KeepPolicyFunc, error) {
	if min < 0 {
		return nil, fmt.Errorf("negative max in compaction.MinVersions: %d", min)
	}
	return func(versions []store.StateVersion) []store.StateVersion {
		if len(versions) < min {
			return versions
		}
		return versions[len(versions)-min:]
	}, nil
}

type RemovePolicyFunc func([]store.StateVersion) []store.StateVersion

func RemovePolicy(policy RemovePolicyFunc) StrategyOption {
	return func(compacter *Compacter) error {
		compacter.removePolicies = append(compacter.removePolicies, policy)
		return nil
	}
}

type KeepPolicyFunc func([]store.StateVersion) []store.StateVersion

func KeepPolicy(policy KeepPolicyFunc) StrategyOption {
	return func(compacter *Compacter) error {
		compacter.keepPolicies = append(compacter.keepPolicies, policy)
		return nil
	}
}
