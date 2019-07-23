package state

// State the key-value storage interface
type State interface {
	Get(key string) (interface{}, bool, error)
	MustGet(key string) interface{}
	Set(key string, val interface{}) error
	GetOrSet(key string, factory GetOrSetFactoryFunc) (interface{}, bool, error)
	Del(key string)
	Range(iter IteratorFunc) error
	String() string
	Invoke(key string, callback InvokeFunc) error
	Lock()
	Unlock()
}

// GetOrSetFactoryFunc will return a val to set if the Get returns nothing
type GetOrSetFactoryFunc func() interface{}

// IteratorFunc is the iterator for state ranges
type IteratorFunc func(key string, val interface{}, kv map[string]interface{}) error

// InvokeFunc is used as callback for Invoke
type InvokeFunc func(val interface{}, kv map[string]interface{}) error
