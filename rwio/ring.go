package rwio

import "sync"

// Ring represents a circular buffer that stores items of type T.
// The items can be added to the buffer, and accessed in a circular fashion.
type Ring[T any] struct {
	mu    sync.RWMutex
	items []T
	index int
}

// NewRing creates a new Ring with the given initial items.
// The items are added to the buffer in the order they are passed.
func NewRing[T any](items ...T) *Ring[T] {
	return &Ring[T]{items: items, index: 0, mu: sync.RWMutex{}}
}

// Add adds the given item to the Ring.
func (r *Ring[T]) Add(item T) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.items = append(r.items, item)
}

// get returns the item at the given index in the Ring.
// If the Ring is empty, Get returns a zero value of type T.
// If the index is out of range, Get returns an item from the start of the Ring.
func (r *Ring[T]) get(index int) T {
	if len(r.items) == 0 {
		panic("cannot get item from empty ring")
	}
	return r.items[index%len(r.items)]
}

// Next returns the next item in the Ring.
// The item is determined by the current index in the Ring.
// The index is incremented after the item is returned.
func (r *Ring[T]) Next() T {
	r.mu.Lock()
	defer r.mu.Unlock()
	item := r.get(r.index)
	r.index++
	return item
}
