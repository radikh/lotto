package rwio

import (
	"io"
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

type WriterIterator interface {
	Next() io.Writer
}

type WriterPoolDescriptorRecorder struct {
	WriterPool
	id string
}

func NewWriterPoolRecorder(pool WriterPool) *WriterPoolDescriptorRecorder {
	return &WriterPoolDescriptorRecorder{
		WriterPool: pool,
	}
}

func (r *WriterPoolDescriptorRecorder) Get() io.Writer {
	reference := uuid.New().String()
	writer := r.WriterPool.Get(reference)
	return NewWriterDescriptorRecorder(writer, reference)
}

type WriterDescriptorRecorder struct {
	io.Writer
	descriptor FileDescriptor
}

func NewWriterDescriptorRecorder(writer io.Writer, descriptor FileDescriptor) *WriterDescriptorRecorder {
	return &WriterDescriptorRecorder{
		Writer:     writer,
		descriptor: descriptor,
	}
}

func (r *WriterDescriptorRecorder) Write(p []byte) (n int, err error) {
	n, err = r.Writer.Write(p)
	r.descriptor.Size += int64(n)
	return n, err
}

func NewRingIterator(writers []io.Writer) WriterIterator {
	ring := NewRing[io.Writer]()
	for _, writer := range writers {
		ring.Add(writer)
	}
	return ring
}

type IteratorConstructor func(writers []io.Writer) WriterIterator

type LimitWriterIteratorWrapper struct {
	FragmentLength int
	SubIterator    WriterIterator
}

func NewLimitWriterIteratorWrapper(subIterator WriterIterator, fragmentLength int) *LimitWriterIteratorWrapper {
	return &LimitWriterIteratorWrapper{
		FragmentLength: fragmentLength,
		SubIterator:    subIterator,
	}
}

func (i *LimitWriterIteratorWrapper) Next() io.Writer {
	next := i.SubIterator.Next()
	return NewShortWriteWriter(next, i.FragmentLength)
}

type ShortWriteWriter struct {
	w     io.Writer
	limit int
}

func NewShortWriteWriter(w io.Writer, limit int) *ShortWriteWriter {
	return &ShortWriteWriter{
		w:     w,
		limit: limit,
	}
}

func (sw *ShortWriteWriter) Write(p []byte) (n int, err error) {
	if sw.limit <= 0 {
		return 0, io.ErrShortWrite
	}
	if len(p) > sw.limit {
		p = p[:sw.limit]
		err = io.ErrShortWrite
	}
	n, err = sw.w.Write(p)
	sw.limit -= n
	return n, err
}

// FragmentWriter is a writer that writes
// to a pool of writers in a round-robin fashion.
type FragmentWriter struct {
	overflow              int
	fragmentLength        int
	chunkWriterDescriptor FileDescriptor
	pool                  *WriterPool
	iterator              *Ring[string]
	mutex                 sync.Mutex
}

// NewRoundRobinChunkWriter creates a new RoundRobinChunkWriter.
func NewRoundRobinChunkWriter(pool *WriterPool, writersNumber, fragmentLength int) (*FragmentWriter, error) {
	writers := NewRing[string]()
	for i := 0; i < writersNumber; i++ {
		writers.Add(uuid.New().String())
	}

	return &FragmentWriter{
		chunkWriterDescriptor: FileDescriptor{
			Fragments: make(map[int]Fragment),
		},
		fragmentLength: fragmentLength,
		iterator:       writers,
		pool:           pool,
		mutex:          sync.Mutex{},
	}, nil
}

// Write writes the given data to the pool of writers in a round-robin fashion.
func (c *FragmentWriter) Write(p []byte) (int, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	written := 0
	for i := 0; len(p) > 0; i++ {
		writerID := c.iterator.Next()
		data := p[:c.fragmentLength-c.overflow]
		n, err := c.pool.Get(writerID).Write(data)
		if err != nil {
			return written, err
		}
		written += n
		p = p[n:]

		c.overflow = c.fragmentLength - n

		c.chunkWriterDescriptor.Fragments[i] = Fragment{
			Location: writerID,
			Length:   n,
		}
	}
	return written, nil
}

// Descriptor returns the FileDescriptor of the file that was written.
func (c *FragmentWriter) Descriptor() FileDescriptor {
	return c.chunkWriterDescriptor
}
