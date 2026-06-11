package worker

import (
	"sort"
	"sync/atomic"
)

// pairSet is a copy-on-write set of WS pair strings, race-safe for readers.
// Used by the Worker to expose the current subscription universe to the
// readLoop and reconnect path without holding a mutex on the hot path.
type pairSet struct {
	ptr atomic.Pointer[[]string]
}

// newPairSet constructs a set seeded with the given pairs. The seed is
// defensively copied + sorted.
func newPairSet(initial []string) *pairSet {
	s := &pairSet{}
	s.Store(initial)
	return s
}

// Load returns a defensive copy of the current set, sorted.
func (s *pairSet) Load() []string {
	ptr := s.ptr.Load()
	if ptr == nil {
		return nil
	}
	out := make([]string, len(*ptr))
	copy(out, *ptr)
	return out
}

// Store atomically replaces the set with a sorted, deduplicated copy of pairs.
func (s *pairSet) Store(pairs []string) {
	dedup := dedupAndSort(pairs)
	s.ptr.Store(&dedup)
}

// Contains reports whether pair is in the current set.
func (s *pairSet) Contains(pair string) bool {
	cur := s.Load()
	for _, p := range cur {
		if p == pair {
			return true
		}
	}
	return false
}

// Add atomically inserts pair into the set. No-op if already present.
// Returns true if pair was actually added.
func (s *pairSet) Add(pair string) bool {
	cur := s.Load()
	for _, p := range cur {
		if p == pair {
			return false
		}
	}
	cur = append(cur, pair)
	sort.Strings(cur)
	s.ptr.Store(&cur)
	return true
}

// Remove atomically removes pair from the set. No-op if not present.
// Returns true if pair was actually removed.
func (s *pairSet) Remove(pair string) bool {
	cur := s.Load()
	out := cur[:0]
	removed := false
	for _, p := range cur {
		if p == pair {
			removed = true
			continue
		}
		out = append(out, p)
	}
	if !removed {
		return false
	}
	cp := make([]string, len(out))
	copy(cp, out)
	s.ptr.Store(&cp)
	return true
}

// Diff computes (toAdd, toRemove) such that applying the diff to the current
// set yields the desired set. Both result slices are sorted.
func (s *pairSet) Diff(desired []string) (toAdd, toRemove []string) {
	desiredSet := make(map[string]struct{}, len(desired))
	for _, p := range desired {
		desiredSet[p] = struct{}{}
	}
	cur := s.Load()
	curSet := make(map[string]struct{}, len(cur))
	for _, p := range cur {
		curSet[p] = struct{}{}
	}
	for p := range desiredSet {
		if _, ok := curSet[p]; !ok {
			toAdd = append(toAdd, p)
		}
	}
	for p := range curSet {
		if _, ok := desiredSet[p]; !ok {
			toRemove = append(toRemove, p)
		}
	}
	sort.Strings(toAdd)
	sort.Strings(toRemove)
	return toAdd, toRemove
}

func dedupAndSort(pairs []string) []string {
	seen := make(map[string]struct{}, len(pairs))
	out := make([]string, 0, len(pairs))
	for _, p := range pairs {
		if _, ok := seen[p]; ok {
			continue
		}
		seen[p] = struct{}{}
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}
