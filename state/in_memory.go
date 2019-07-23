package state

import (
	"strings"
	"sync"

	pjson "github.com/pinpt/go-common/json"
)

// InMemoryState memory state
type InMemoryState struct {
	kv     map[string]interface{}
	mu     sync.RWMutex
	prefix string
}

// NewInMemoryState initializes the InMemoryState
func NewInMemoryState(prefix ...string) *InMemoryState {
	state := &InMemoryState{
		kv: make(map[string]interface{}),
	}
	if len(prefix) > 0 {
		state.prefix = prefix[0]
	}
	return state
}

// Get gets a value, if exists
func (m *InMemoryState) Get(key string) (interface{}, bool, error) {
	m.mu.RLock()
	val, ok := m.kv[m.prefix+key]
	m.mu.RUnlock()
	return val, ok, nil
}

// MustGet gets a value, or panics
func (m *InMemoryState) MustGet(key string) interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()
	val, ok := m.kv[m.prefix+key]
	if !ok {
		panic("required key " + key + " not found in state")
	}
	return val
}

// Set sets a value
func (m *InMemoryState) Set(key string, val interface{}) error {
	m.Lock()
	m.kv[m.prefix+key] = val
	m.Unlock()
	return nil
}

// GetOrSet self explanatory
func (m *InMemoryState) GetOrSet(key string, factory GetOrSetFactoryFunc) (interface{}, bool, error) {
	m.Lock()
	defer m.Unlock()
	val, ok := m.kv[m.prefix+key]
	if !ok {
		val = factory()
		m.kv[m.prefix+key] = val
	}
	return val, ok, nil

}

// Del removes a value
func (m *InMemoryState) Del(key string) {
	m.Lock()
	delete(m.kv, m.prefix+key)
	m.Unlock()

}

// Range iterates overs the storage calling a function callback on every iteration
func (m *InMemoryState) Range(iter IteratorFunc) error {
	m.Lock()
	defer m.Unlock()
	for k, v := range m.kv {
		if strings.HasPrefix(k, m.prefix) {
			k = strings.Replace(k, m.prefix, "", 1)
		}
		if err := iter(k, v, m.kv); err != nil {
			return err
		}
	}
	return nil

}

// String json representation of the storage
func (m *InMemoryState) String() string {
	m.mu.RLock()
	val := pjson.Stringify(m.kv)
	m.mu.RUnlock()
	return val

}

// Invoke invokes the InvokeFunc on a value
func (m *InMemoryState) Invoke(key string, callback InvokeFunc) error {
	m.Lock()
	defer m.Unlock()
	val, ok := m.kv[m.prefix+key]
	if ok {
		if err := callback(val, m.kv); err != nil {
			return err
		}
	} else {
		if err := callback(nil, m.kv); err != nil {
			return err
		}
	}
	return nil

}

// Lock the state for writing
func (m *InMemoryState) Lock() {
	m.mu.Lock()
}

// rlock the state for writing
func (m *InMemoryState) rlock() {
	m.mu.RLock()
}

// Unlock the state
func (m *InMemoryState) Unlock() {
	m.mu.Unlock()
}

// runlock the state
func (m *InMemoryState) runlock() {
	m.mu.RUnlock()
}
