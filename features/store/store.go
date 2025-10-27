package store

import (
	"sync"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/features"
)

// Store holds closable resources for an Instance.
// Resources are automatically closed when the Instance closes.
type Store struct {
	sync.RWMutex
	items map[string]common.Closable
}

// Type implements common.HasType.
func (*Store) Type() interface{} {
	return Type()
}

// Start implements common.Runnable.
func (s *Store) Start() error {
	return nil
}

// Close implements common.Runnable and closes all resources in the store.
func (s *Store) Close() error {
	s.Lock()
	defer s.Unlock()

	for key, it := range s.items {
		it.Close()
		delete(s.items, key)
	}

	return nil
}

// New creates a new resource store.
func New() *Store {
	return &Store{
		items: make(map[string]common.Closable),
	}
}

// Put stores a resource in the store.
// If the key already exists, the old resource is closed first.
func (s *Store) Put(key string, resource common.Closable) {
	s.Lock()
	defer s.Unlock()

	// If key already exists, close the old resource
	if old, exists := s.items[key]; exists {
		old.Close()
	}

	s.items[key] = resource
}

// Get retrieves a resource from the store.
func (s *Store) Get(key string) (common.Closable, bool) {
	s.RLock()
	defer s.RUnlock()
	it, found := s.items[key]
	return it, found
}

// Delete removes and closes a resource from the store.
func (s *Store) Delete(key string) bool {
	s.Lock()
	defer s.Unlock()

	it, exists := s.items[key]
	if !exists {
		return false
	}

	it.Close()
	delete(s.items, key)

	return true
}

// Type returns the type of Store.
func Type() interface{} {
	return (*Store)(nil)
}

var _ features.Feature = (*Store)(nil)
