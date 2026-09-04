package kernel

import "sync"

// Scope is the persistent user namespace shared by all cells. Concurrent
// cell execution (v4 policy) mutates it under the write lock; snapshots and
// list_names take the read lock and never block writers longer than the copy.
type Scope struct {
	mu    sync.RWMutex
	names map[string]any
}

func NewScope() *Scope { return &Scope{names: make(map[string]any)} }

func (s *Scope) Set(k string, v any) {
	s.mu.Lock()
	s.names[k] = v
	s.mu.Unlock()
}

func (s *Scope) Get(k string) (any, bool) {
	s.mu.RLock()
	v, ok := s.names[k]
	s.mu.RUnlock()
	return v, ok
}

func (s *Scope) Names() []string {
	s.mu.RLock()
	out := make([]string, 0, len(s.names))
	for k := range s.names {
		out = append(out, k)
	}
	s.mu.RUnlock()
	// sorted by caller
	return out
}

// Entries copies the namespace under the read lock; snapshot serializes
// off the critical path.
func (s *Scope) Entries() map[string]any {
	s.mu.RLock()
	out := make(map[string]any, len(s.names))
	for k, v := range s.names {
		out[k] = v
	}
	s.mu.RUnlock()
	return out
}

func (s *Scope) Delete(k string) {
	s.mu.Lock()
	delete(s.names, k)
	s.mu.Unlock()
}
