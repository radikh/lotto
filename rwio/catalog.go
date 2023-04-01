package rwio

import (
	"bytes"
	"errors"
	"io"
	"sync"
)

// ErrNotFound is returned when the requested catalog reference is not found.
var ErrNotFound = errors.New("not found")

// Catalog is a primitive filesystem.
type Catalog interface {
	Open(reference string) (io.ReadCloser, error)
	Create(reference string) (io.WriteCloser, error)
	Remove(reference string) error
	Rename(old, new string) error
	Exists(reference string) (bool, error)
}

// InMemoryCatalog is an implementation of Catalog that stores files in memory.
type InMemoryCatalog struct {
	buffers map[string]*bytes.Buffer
	mu      sync.RWMutex
}

// Open opens an existing file for reading. Returns ErrNotFound if the file is not found.
func (c *InMemoryCatalog) Open(reference string) (io.ReadCloser, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if buffer, ok := c.buffers[reference]; ok {
		return io.NopCloser(bytes.NewReader(buffer.Bytes())), nil
	}

	return nil, ErrNotFound
}

// Create creates a new file for writing. Returns an error if the file already exists.
func (c *InMemoryCatalog) Create(reference string) (io.WriteCloser, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.buffers[reference]; ok {
		return nil, errors.New("already exists")
	}

	buffer := &bytes.Buffer{}
	c.buffers[reference] = buffer

	return NopCloserWriter(buffer), nil
}

// Remove removes an existing file. Returns ErrNotFound if the file is not found.
func (c *InMemoryCatalog) Remove(reference string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.buffers[reference]; ok {
		delete(c.buffers, reference)
		return nil
	}

	return ErrNotFound
}

// Rename renames an existing file. Returns ErrNotFound if the file is not found.
func (c *InMemoryCatalog) Rename(old, new string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.buffers[old]; ok {
		c.buffers[new] = c.buffers[old]
		delete(c.buffers, old)
		return nil
	}

	return ErrNotFound
}

// Exists checks if a file exists.
func (c *InMemoryCatalog) Exists(reference string) (bool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if _, ok := c.buffers[reference]; ok {
		return true, nil
	}

	return false, nil
}

// Create a function to instantiate a new instance of your catalog
func NewInMemoryCatalog() *InMemoryCatalog {
	return &InMemoryCatalog{
		buffers: make(map[string]*bytes.Buffer),
		mu:      sync.RWMutex{},
	}
}
