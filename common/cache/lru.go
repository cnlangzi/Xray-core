package cache

import (
	"container/list"
	"sync"
)

type EvictCallback func(key, value any)

// Lru simple, fast lru cache implementation
type Lru interface {
	Get(key interface{}) (value interface{}, ok bool)
	GetKeyFromValue(value interface{}) (key interface{}, ok bool)
	PeekKeyFromValue(value interface{}) (key interface{}, ok bool) // Peek means check but NOT bring to top
	Put(key, value interface{})
	Delete(key interface{})
}

type lru struct {
	capacity         int
	doubleLinkedlist *list.List
	keyToElement     *sync.Map
	valueToElement   *sync.Map
	mu               *sync.Mutex
	onEvicted        func(key, value interface{})
}

type lruElement struct {
	key   interface{}
	value interface{}
}

// NewLru initializes a lru cache
func NewLru(cap int) Lru {
	return NewLruWith(cap, nil)
}

func NewLruWith(cap int, onEvicted EvictCallback) Lru {
	return &lru{
		capacity:         cap,
		doubleLinkedlist: list.New(),
		keyToElement:     new(sync.Map),
		valueToElement:   new(sync.Map),
		mu:               new(sync.Mutex),
		onEvicted:        onEvicted,
	}
}

func (l *lru) Get(key interface{}) (value interface{}, ok bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if v, ok := l.keyToElement.Load(key); ok {
		element := v.(*list.Element)
		l.doubleLinkedlist.MoveToFront(element)
		return element.Value.(*lruElement).value, true
	}
	return nil, false
}

func (l *lru) GetKeyFromValue(value interface{}) (key interface{}, ok bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if k, ok := l.valueToElement.Load(value); ok {
		element := k.(*list.Element)
		l.doubleLinkedlist.MoveToFront(element)
		return element.Value.(*lruElement).key, true
	}
	return nil, false
}

func (l *lru) PeekKeyFromValue(value interface{}) (key interface{}, ok bool) {
	if k, ok := l.valueToElement.Load(value); ok {
		element := k.(*list.Element)
		return element.Value.(*lruElement).key, true
	}
	return nil, false
}

func (l *lru) Put(key, value interface{}) {
	l.mu.Lock()
	e := &lruElement{key, value}
	var cb func(key, value interface{})
	var removedKey, removedValue interface{}

	if v, ok := l.keyToElement.Load(key); ok {
		element := v.(*list.Element)
		// update value
		element.Value = e
		l.doubleLinkedlist.MoveToFront(element)
		// NOTE: valueToElement map isn't updated for old value in existing behavior.
		// To preserve current semantics, we do not modify valueToElement here.
	} else {
		element := l.doubleLinkedlist.PushFront(e)
		l.keyToElement.Store(key, element)
		l.valueToElement.Store(value, element)
		if l.doubleLinkedlist.Len() > l.capacity {
			toBeRemove := l.doubleLinkedlist.Back()
			if toBeRemove != nil {
				// capture key/value before removal
				if entry, ok := toBeRemove.Value.(*lruElement); ok {
					removedKey, removedValue = entry.key, entry.value
				}
				l.doubleLinkedlist.Remove(toBeRemove)
				if entry, ok := toBeRemove.Value.(*lruElement); ok {
					l.keyToElement.Delete(entry.key)
					l.valueToElement.Delete(entry.value)
				}
				if l.onEvicted != nil && removedKey != nil {
					cb = l.onEvicted
				}
			}
		}
	}
	l.mu.Unlock()

	// Invoke callback outside the lock to avoid potential deadlocks
	if cb != nil {
		cb(removedKey, removedValue)
	}
}

func (l *lru) Delete(key interface{}) {
	l.mu.Lock()
	var cb func(key, value interface{})
	var removedKey, removedValue interface{}

	if v, ok := l.keyToElement.Load(key); ok {
		element := v.(*list.Element)
		l.doubleLinkedlist.Remove(element)
		l.keyToElement.Delete(key)
		if entry, ok := element.Value.(*lruElement); ok {
			removedKey, removedValue = entry.key, entry.value
			l.valueToElement.Delete(entry.value)
		}
		if l.onEvicted != nil {
			cb = l.onEvicted
		}
	}
	l.mu.Unlock()

	if cb != nil {
		cb(removedKey, removedValue)
	}
}
