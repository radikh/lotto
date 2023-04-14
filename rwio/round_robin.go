package rwio

import (
	"errors"
	"io"
	"sync"

	"github.com/google/uuid"
)

// WriterIterator is an interface that allows to iterate over a collection of io.Writer.
type WriterIterator interface {
	Next() io.Writer
}

// WriterFactory is a wrapper around WriterPool
// that spawns writers able to fill the File Descriptor when writing.
type WriterFactory struct {
	*WriterPool
	recorder *DescriptorRecorder
}

// NewWriterFactory creates a new WriterPoolDescriptorRecorder.
func NewWriterFactory(pool *WriterPool, recorder *DescriptorRecorder) *WriterFactory {
	return &WriterFactory{
		WriterPool: pool,
		recorder:   recorder,
	}
}

// Get returns a new writer that is able to fill the File Descriptor when writing.
func (r *WriterFactory) Get() io.Writer {
	reference := uuid.New().String()
	writer := r.WriterPool.Get(reference)
	return r.NewWriterDescriptorRecorder(writer, reference)
}

// WriterDescriptorRecorder is a wrapper around io.Writer
// that records the fragments written to the underlying writer.
// It is used to fill the File Descriptor.
type WriterDescriptorRecorder struct {
	io.Writer
	recorder  *DescriptorRecorder
	reference string
}

// NewWriterDescriptorRecorder creates a new WriterDescriptorRecorder.
func (r *WriterFactory) NewWriterDescriptorRecorder(writer io.Writer, reference string) *WriterDescriptorRecorder {
	return &WriterDescriptorRecorder{
		Writer:    writer,
		recorder:  r.recorder,
		reference: reference,
	}
}

// Write writes the given data to the underlying writer
// and records the fragment to the File Descriptor.
func (r *WriterDescriptorRecorder) Write(p []byte) (n int, err error) {
	n, err = r.Writer.Write(p)
	fragment := Fragment{
		Length:   n,
		Location: r.reference,
	}

	r.recorder.Record(fragment)

	return n, err
}

// RoundRobinChunkWriter is a writer that writes
// to the underlying writers in a round robin fashion.
type LimitWriterIteratorWrapper struct {
	FragmentLength int
	SubIterator    WriterIterator
}

// NewRoundRobinChunkWriter creates a new RoundRobinChunkWriter.
func NewLimitWriterIteratorWrapper(subIterator WriterIterator, fragmentLength int) *LimitWriterIteratorWrapper {
	return &LimitWriterIteratorWrapper{
		FragmentLength: fragmentLength,
		SubIterator:    subIterator,
	}
}

// Next returns the next writer in the pool
// limiting the writes to the fragment length.
func (i *LimitWriterIteratorWrapper) Next() io.Writer {
	next := i.SubIterator.Next()
	return NewShortWriteWriter(next, i.FragmentLength)
}

// ShortWriteWriter is a writer that writes
// to the underlying writer and returns io.ErrShortWrite
// if the data is larger than the limit.
type ShortWriteWriter struct {
	w     io.Writer
	limit int
}

// NewShortWriteWriter creates a new ShortWriteWriter.
func NewShortWriteWriter(w io.Writer, limit int) *ShortWriteWriter {
	return &ShortWriteWriter{
		w:     w,
		limit: limit,
	}
}

// Write writes the given data to the underlying writer.
// If the data is larger than the limit, it will be
// truncated and io.ErrShortWrite will be returned.
func (sw *ShortWriteWriter) Write(p []byte) (n int, err error) {
	if sw.limit <= 0 {
		return 0, io.ErrShortWrite
	}

	var errShort error
	if len(p) > sw.limit {
		p = p[:sw.limit]
		errShort = io.ErrShortWrite
	}

	n, err = sw.w.Write(p)
	sw.limit -= n
	return n, errors.Join(err, errShort)
}

// RoundRobinChunkWriter is a writer that writes
// writes to the underlying writers until the data is exhausted.
type SequenceWriter struct {
	iterator WriterIterator
	mutex    sync.Mutex
}

// NewSequenceWriter creates a new SequenceWriter.
func NewSequenceWriter(iterator WriterIterator) *SequenceWriter {
	return &SequenceWriter{
		iterator: iterator,
		mutex:    sync.Mutex{},
	}
}

// Write writes the given data to the pool of writers in a round-robin fashion.
func (c *SequenceWriter) Write(p []byte) (int, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	written := 0
	for i := 0; len(p) > 0; i++ {
		n, err := c.iterator.Next().Write(p)
		written += n
		p = p[n:]
		if errors.Is(err, io.ErrShortWrite) {
			continue
		}
		if err != nil {
			return written, err
		}
	}
	return written, nil
}
