package rwio

import (
	"io"
	"sync"
)

// FragmentReader is used to read fragments from a file described by a FileDescriptor.
type FragmentReader struct {
	readers    *ReaderPool
	descriptor *FileDescriptor
	position   int
	offset     int
	mu         sync.RWMutex
}

// NewFragmentReader creates a new FragmentReader.
func NewFragmentReader(catalog Catalog, descriptor *FileDescriptor) *FragmentReader {
	return &FragmentReader{
		readers:    NewReaderPool(catalog),
		descriptor: descriptor,
		position:   0,
		offset:     0,
	}
}

// Read reads the data from the infrastructure using the FileDescriptor provided.
func (c *FragmentReader) Read(buf []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	written := 0

	for _, fragment := range c.descriptor.Fragments[c.position:] {
		max := fragment.Length - c.offset
		window := cutToMaxLen(buf, max)

		reader := c.readers.Get(fragment.Location)
		n, err := reader.Read(window)
		if err != nil {
			return written, err
		}

		c.offset += n
		if c.offset >= fragment.Length {
			c.position++
			c.offset = 0
		}

		buf = buf[n:]
		written += n
	}

	if written < len(buf) {
		return written, io.EOF
	}

	return written, nil
}

func cutToMaxLen[T any](slice []T, maxLen int) []T {
	if len(slice) > maxLen {
		return slice[:maxLen]
	}
	return slice
}
