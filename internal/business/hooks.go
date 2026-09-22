package business

import "sync"

// MemoryHooks is a concurrency-safe FlakyHooks implementation.
type MemoryHooks struct {
	mu      sync.Mutex
	failFor map[string]int
	calls   map[string]int
}

// NewHooks creates an empty hook set.
func NewHooks() *MemoryHooks {
	return &MemoryHooks{failFor: make(map[string]int), calls: make(map[string]int)}
}

// FailNext arranges for id's first n attempts to fail transiently.
func (h *MemoryHooks) FailNext(id string, n int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.failFor[id] = n
}

func (h *MemoryHooks) FailNTimes(id string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.failFor[id]
}

func (h *MemoryHooks) RecordAttempt(id string, attempt int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.calls[id] = attempt
}

// Attempts returns the highest attempt number observed for id.
func (h *MemoryHooks) Attempts(id string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.calls[id]
}
