package validator

import (
	"sync"
)

// InFlightTracker keeps track of document IDs that are currently being processed
// by the CDC stream. If an ID is "Dirty", it cannot be safely validated.
type InFlightTracker struct {
	mu       sync.RWMutex
	dirtyIDs map[string]bool
}

// NewInFlightTracker creates a new tracker instance
func NewInFlightTracker() *InFlightTracker {
	return &InFlightTracker{
		dirtyIDs: make(map[string]bool),
	}
}

// MarkDirty is called by the CDC Worker whenever it processes an event for an ID
func (t *InFlightTracker) MarkDirty(id string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.dirtyIDs[id] = true
}

// IsDirty checks if an ID was modified recently
func (t *InFlightTracker) IsDirty(id string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.dirtyIDs[id]
}

// ClearDirty resets the state for a specific ID (used before validation starts)
func (t *InFlightTracker) ClearDirty(id string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.dirtyIDs, id)
}
