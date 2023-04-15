package rwio

import (
	"io"
	"sync"
)

// Pool is a generic pool of io.Closer instances.
type Pool[item io.Closer] struct {
	items       map[string]item
	constructor Constructor[item]
	mu          sync.RWMutex
}

// Constructor is a function serving a pool that creates a new instance of an io.Closer.
type Constructor[item io.Closer] func(reference string) (item, error)

// NewPool creates a new pool.
func NewPool[item io.Closer](constructor Constructor[item]) *Pool[item] {
	return &Pool[item]{
		items:       make(map[string]item),
		constructor: constructor,
	}
}

// Get returns an io.Closer from the pool.
func (p *Pool[item]) Get(reference string) (item, error) {
	p.mu.RLock()
	result, ok := p.items[reference]
	p.mu.RUnlock()

	if !ok {
		rc, err := p.constructor(reference)
		if err != nil {
			return *new(item), err
		}

		p.mu.Lock()
		p.items[reference] = rc
		p.mu.Unlock()

		result = rc
	}

	return result, nil
}

// Delete removes an io.Closer from the pool.
func (p *Pool[item]) Delete(reference string) {
	p.mu.Lock()
	entry, ok := p.items[reference]
	if ok {
		delete(p.items, reference)
	}
	p.mu.Unlock()

	entry.Close()
}

// Close closes all io.Closer instances in the pool.
func (p *Pool[item]) Close() {
	p.mu.Lock()
	for _, entry := range p.items {
		entry.Close()
	}
	p.mu.Unlock()
}
