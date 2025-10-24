package cache_test

import (
	"testing"

	. "github.com/xtls/xray-core/common/cache"
)

func TestLruReplaceValue(t *testing.T) {
	lru := NewLru(2)
	lru.Put(2, 6)
	lru.Put(1, 5)
	lru.Put(1, 2)
	v, _ := lru.Get(1)
	if v != 2 {
		t.Error("should get 2", v)
	}
	v, _ = lru.Get(2)
	if v != 6 {
		t.Error("should get 6", v)
	}
}

func TestLruRemoveOld(t *testing.T) {
	lru := NewLru(2)
	v, ok := lru.Get(2)
	if ok {
		t.Error("should get nil", v)
	}
	lru.Put(1, 1)
	lru.Put(2, 2)
	v, _ = lru.Get(1)
	if v != 1 {
		t.Error("should get 1", v)
	}
	lru.Put(3, 3)
	v, ok = lru.Get(2)
	if ok {
		t.Error("should get nil", v)
	}
	lru.Put(4, 4)
	v, ok = lru.Get(1)
	if ok {
		t.Error("should get nil", v)
	}
	v, _ = lru.Get(3)
	if v != 3 {
		t.Error("should get 3", v)
	}
	v, _ = lru.Get(4)
	if v != 4 {
		t.Error("should get 4", v)
	}
}

func TestGetKeyFromValue(t *testing.T) {
	lru := NewLru(2)
	lru.Put(3, 3)
	lru.Put(2, 2)
	lru.GetKeyFromValue(3)
	lru.Put(1, 1)
	v, ok := lru.GetKeyFromValue(2)
	if ok {
		t.Error("should get nil", v)
	}
	v, _ = lru.GetKeyFromValue(3)
	if v != 3 {
		t.Error("should get 3", v)
	}
}

func TestPeekKeyFromValue(t *testing.T) {
	lru := NewLru(2)
	lru.Put(3, 3)
	lru.Put(2, 2)
	lru.PeekKeyFromValue(3)
	lru.Put(1, 1)
	v, ok := lru.PeekKeyFromValue(3)
	if ok {
		t.Error("should get nil", v)
	}
	v, _ = lru.PeekKeyFromValue(2)
	if v != 2 {
		t.Error("should get 2", v)
	}
}

func TestLruOnEvictedOnCapacity(t *testing.T) {
	l := NewLru(1)

	type pair struct {
		k interface{}
		v interface{}
	}
	evicted := make(chan pair, 1)

	l.OnEvicted(func(key, value interface{}) {
		evicted <- pair{k: key, v: value}
	})

	l.Put("a", 1)
	l.Put("b", 2) // should evict a

	select {
	case p := <-evicted:
		if p.k != "a" || p.v != 1 {
			t.Fatalf("unexpected eviction payload: %v", p)
		}
	default:
		t.Fatal("expected eviction callback to be called")
	}
}

func TestLruOnEvictedOnDelete(t *testing.T) {
	l := NewLru(2)

	type pair struct {
		k interface{}
		v interface{}
	}
	evicted := make(chan pair, 1)

	l.OnEvicted(func(key, value interface{}) {
		evicted <- pair{k: key, v: value}
	})

	l.Put("x", 100)
	l.Delete("x")

	select {
	case p := <-evicted:
		if p.k != "x" || p.v != 100 {
			t.Fatalf("unexpected deletion payload: %v", p)
		}
	default:
		t.Fatal("expected deletion callback to be called")
	}
}
