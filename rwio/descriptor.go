package rwio

import (
	"errors"
	"sync"
)

var (
	ErrUnknownDescriptor       = errors.New("unknown descriptor")
	ErrDescriptorAlreadyExists = errors.New("descriptor already exists")
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

func (fd *FileDescriptor) IsEmpty() bool {
	return len(fd.Fragments) == 0
}

// NewFileDescriptor creates a new FileDescriptor.
func NewFileDescriptor() *FileDescriptor {
	return &FileDescriptor{
		Fragments: make([]Fragment, 0),
	}
}

// DescriptorRecorder is used to record the fragments of a file.
type DescriptorRecorder struct {
	mu   sync.RWMutex
	desc *FileDescriptor
}

// NewDescriptorRecorder creates a new DescriptorRecorder.
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
}

// DescriptorPool is used to store file descriptors.
type DescriptorPool struct {
	mu   sync.RWMutex
	pool map[string]*FileDescriptor
}

// NewDescriptorPool creates a new DescriptorPool.
func NewDescriptorPool() *DescriptorPool {
	return &DescriptorPool{
		pool: make(map[string]*FileDescriptor),
	}
}

// Get returns a FileDescriptor from the pool.
// If the descriptor does not exist, it creates a new one.
func (dp *DescriptorPool) Get(reference string) *FileDescriptor {
	dp.mu.RLock()
	defer dp.mu.RUnlock()

	descriptor, ok := dp.pool[reference]
	if !ok {
		descriptor = NewFileDescriptor()
		dp.pool[reference] = descriptor
	}

	return descriptor
}

func (dp *DescriptorPool) Remove(reference string) {
	dp.mu.Lock()
	defer dp.mu.Unlock()

	delete(dp.pool, reference)
}

// Rename renames a FileDescriptor.
func (dp *DescriptorPool) Rename(old, new string) error {
	dp.mu.Lock()
	defer dp.mu.Unlock()

	if _, ok := dp.pool[old]; !ok {
		return ErrUnknownDescriptor
	}

	if _, ok := dp.pool[new]; ok {
		return ErrDescriptorAlreadyExists
	}

	dp.pool[new] = dp.pool[old]
	delete(dp.pool, old)

	return nil
}

func (dp *DescriptorPool) List() []string {
	dp.mu.RLock()
	defer dp.mu.RUnlock()

	list := make([]string, 0, len(dp.pool))
	for reference := range dp.pool {
		list = append(list, reference)
	}

	return list
}

func (dp *DescriptorPool) Len() int {
	dp.mu.RLock()
	defer dp.mu.RUnlock()

	return len(dp.pool)
}

func (dp *DescriptorPool) Dump() map[string]*FileDescriptor {
	dp.mu.RLock()
	defer dp.mu.RUnlock()

	dump := make(map[string]*FileDescriptor, len(dp.pool))
	for reference, descriptor := range dp.pool {
		dump[reference] = descriptor
	}

	return dump
}
