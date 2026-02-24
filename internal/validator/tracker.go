package validator

import (
	"sync"
)

type InFlightTracker struct {
	mu           sync.RWMutex
	dirtyIDs     map[string]bool
	clearedCount int
}

func NewInFlightTracker() *InFlightTracker {
	return &InFlightTracker{
		dirtyIDs: make(map[string]bool),
	}
}

func (t *InFlightTracker) MarkDirty(id string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.dirtyIDs[id] = true
}

func (t *InFlightTracker) IsDirty(id string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.dirtyIDs[id]
}

func (t *InFlightTracker) ClearDirty(id string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	delete(t.dirtyIDs, id)
	t.clearedCount++

	// Reset if we've done significant work, even if not perfectly empty.
	// This prevents the map's internal buckets from staying bloated forever.
	if t.clearedCount > 50000 {
		if len(t.dirtyIDs) < 100 { // Only reset when relatively quiet
			newMap := make(map[string]bool)
			for k, v := range t.dirtyIDs {
				newMap[k] = v
			}
			t.dirtyIDs = newMap
			t.clearedCount = 0
		}
	}
}
func (t *InFlightTracker) Flush() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.dirtyIDs = make(map[string]bool)
	t.clearedCount = 0
}
