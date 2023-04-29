package rwio

import (
	"errors"
	"io"
	"sync"
)

// ReaderPool is a pool of io.ReadCloser instances.
type ReaderPool struct {
	Pool[io.ReadCloser]
}

// NewReaderPool creates a new ReaderPool.
func NewReaderPool(catalog Catalog) *ReaderPool {
	return &ReaderPool{
		Pool: *NewPool(catalog.Open),
	}
}

// Get returns an io.ReadCloser from the pool.
// If connot construct a new instance,
// it returns an ErrorReadWriteCloser with the constructor error.
func (p *ReaderPool) Get(reference string) io.Reader {
	reader, err := p.Pool.Get(reference)
	if err != nil {
		return NewErrorReadWriteCloser(err)
	}

	return reader
}

// FragmentReader is used to read fragments from a file described by a FileDescriptor.
type FragmentReader struct {
	readers *FragmentReaderIterator
	current io.Reader
	mu      sync.RWMutex
}

// FragmentReaderIterator is used to iterate over the fragments of a FileDescriptor.
type FragmentReaderIterator struct {
	readers    *ReaderPool
	descriptor *FileDescriptor
	mu         sync.RWMutex
	index      int
}

// NewFragmentReaderIterator creates a new FragmentReaderIterator.
func NewFragmentReaderIterator(readers *ReaderPool, descriptor *FileDescriptor) *FragmentReaderIterator {
	return &FragmentReaderIterator{
		readers:    readers,
		descriptor: descriptor,
	}
}

// Next returns the next fragment reader.
// If there are no more fragments, it returns nil.
func (fri *FragmentReaderIterator) Next() io.Reader {
	fri.mu.Lock()
	defer fri.mu.Unlock()

	if fri.index >= len(fri.descriptor.Fragments) {
		return nil
	}

	fragment := fri.descriptor.Fragments[fri.index]
	fri.index++

	reader := fri.readers.Get(fragment.Location)

	return &io.LimitedReader{
		R: reader,
		N: int64(fragment.Length),
	}
}

// NewFragmentReader creates a new FragmentReader.
func NewFragmentReader(iterator *FragmentReaderIterator) *FragmentReader {
	return &FragmentReader{
		current: iterator.Next(),
		readers: iterator,
	}
}

// Read reads the data from the infrastructure using the FileDescriptor provided.
func (c *FragmentReader) Read(buf []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	written := 0

	for c.current != nil && len(buf) > 0 {
		n, err := c.current.Read(buf)
		written += n
		if err != nil {
			if errors.Is(err, io.EOF) {
				c.current = c.readers.Next()
				continue
			}

			return written, err
		}
		buf = buf[n:]
	}

	if written < len(buf) {
		return written, io.EOF
	}

	return written, nil
}
