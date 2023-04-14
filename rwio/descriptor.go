package rwio

import "sync"

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
