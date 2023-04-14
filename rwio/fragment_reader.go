package rwio

import (
	"io"
	"sync"
)

// Fragment represents a fragment of a file.
type Fragment struct {
	// Location is the location of the fragment in the catalog.
	Location string
	// Length is the length of the fragment in bytes.
	Length int
}

// FileDescriptor is a scheme of how a file to assemle the file.
type FileDescriptor struct {
	Fragments []Fragment
}

// DescriptorRecorder is used to record the fragments of a file.
type DescriptorRecorder struct {
	mu      sync.RWMutex
	desc    *FileDescriptor
	counter int
}

func NewDescriptorRecorder(descriptor *FileDescriptor) *DescriptorRecorder {
	return &DescriptorRecorder{
		desc: descriptor,
	}
}

// Record records a fragment.
func (cfd *DescriptorRecorder) Record(fragment Fragment) {
	cfd.mu.Lock()
	defer cfd.mu.Unlock()

	cfd.desc.Fragments = append(cfd.desc.Fragments, fragment)

	return
}

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
	for len(buf) > 0 && c.position < len(c.descriptor.Fragments) {
		fragment := c.descriptor.Fragments[c.position]
		max := fragment.Length - c.offset
		window := cutToMaxLen(buf, max)

		reader := c.readers.Get(fragment.Location)
		n, err := reader.Read(window)
		if err != nil {
			return written, err
		}

		c.offset += n
		if c.offset == fragment.Length {
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
