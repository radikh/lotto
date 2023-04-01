package rwio

import (
	"sync"

	"github.com/google/uuid"
)

// Ring represents a circular buffer that stores items of type T.
// The items can be added to the buffer, and accessed in a circular fashion.
type Ring[T any] struct {
	items []T
	index int
}

// NewRing creates a new Ring with the given initial items.
// The items are added to the buffer in the order they are passed.
func NewRing[T any](items ...T) *Ring[T] {
	return &Ring[T]{items: items}
}

// Add adds the given item to the Ring.
func (r *Ring[T]) Add(item T) {
	r.items = append(r.items, item)
}

// Get returns the item at the given index in the Ring.
// If the Ring is empty, Get returns a zero value of type T.
// If the index is out of range, Get returns an item from the start of the Ring.
func (r *Ring[T]) Get(index int) T {
	if len(r.items) == 0 {
		return *new(T)
	}
	return r.items[index%len(r.items)]
}

// Next returns the next item in the Ring.
// The item is determined by the current index in the Ring.
// The index is incremented after the item is returned.
func (r *Ring[T]) Next() T {
	item := r.Get(r.index)
	r.index++
	return item
}

// RoundRobinChunkWriter is a writer that writes
// to a pool of writers in a round-robin fashion.
type RoundRobinChunkWriter struct {
	overflow              int
	chunkSize             int
	chunkWriterDescriptor FileDescriptor
	pool                  *WriterPool
	writers               *Ring[string]
	mutex                 sync.Mutex
}

// NewRoundRobinChunkWriter creates a new RoundRobinChunkWriter.
func NewRoundRobinChunkWriter(pool *WriterPool, writersNumber, chunkSize int) (*RoundRobinChunkWriter, error) {
	writers := NewRing[string]()
	for i := 0; i < writersNumber; i++ {
		writers.Add(uuid.New().String())
	}

	return &RoundRobinChunkWriter{
		chunkWriterDescriptor: FileDescriptor{
			Fragments: make(map[int]Fragment),
		},
		chunkSize: chunkSize,
		writers:   writers,
		pool:      pool,
		mutex:     sync.Mutex{},
	}, nil
}

// Write writes the given data to the pool of writers in a round-robin fashion.
func (c *RoundRobinChunkWriter) Write(p []byte) (int, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	written := 0
	for i := 0; len(p) > 0; i++ {
		writerID := c.writers.Next()
		data := p[:c.chunkSize-c.overflow]
		n, err := c.pool.Get(writerID).Write(data)
		if err != nil {
			return written, err
		}
		written += n
		p = p[n:]

		c.overflow = c.chunkSize - n

		c.chunkWriterDescriptor.Fragments[i] = Fragment{
			Location: writerID,
			Length:   n,
		}
	}
	return written, nil
}

// Descriptor returns the FileDescriptor of the file that was written.
func (c *RoundRobinChunkWriter) Descriptor() FileDescriptor {
	return c.chunkWriterDescriptor
}
