package rwio

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReader(t *testing.T) {
	text := []byte("Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed sed nisl nec nisl luctus lacinia")
	catalog := NewInMemoryCatalog()
	descriptor := func() *FileDescriptor {

		pool := NewWriterPool(catalog)
		descriptor := &FileDescriptor{}
		recorder := NewDescriptorRecorder(descriptor)

		recPool := NewWriterFactory(pool, recorder)

		fragmentLength := 2
		writersNumber := 3
		writers := make([]io.Writer, 0)
		for i := 0; i < writersNumber; i++ {
			writers = append(writers, recPool.Get())
		}

		ringIterator := NewRing(writers...)

		iterator := NewLimitWriterIteratorWrapper(ringIterator, fragmentLength)

		writer := NewFragmentWriter(iterator)

		n, err := writer.Write([]byte(text))
		assert.NoError(t, err)
		assert.Equal(t, len(text), n)

		return descriptor
	}

	t.Run("read_with_eof", func(t *testing.T) {
		readerPool := NewReaderPool(catalog)
		readerIterator := NewFragmentReaderIterator(readerPool, descriptor())

		reader := NewFragmentReader(readerIterator)
		result := make([]byte, 80)

		n, err := reader.Read(result)
		assert.NoError(t, err)
		assert.Equal(t, string(text[:80]), string(result))
		assert.Equal(t, 80, n)
	})

	t.Run("result_bigger", func(t *testing.T) {
		readerPool := NewReaderPool(catalog)
		readerIterator := NewFragmentReaderIterator(readerPool, descriptor())

		reader := NewFragmentReader(readerIterator)
		result := make([]byte, 100)

		n, err := reader.Read(result)
		assert.NoError(t, err)
		assert.Equal(t, string(text), string(result[:n]))
		assert.Equal(t, 93, n)
	})

	t.Run("bigger_cap", func(t *testing.T) {
		readerPool := NewReaderPool(catalog)
		readerIterator := NewFragmentReaderIterator(readerPool, descriptor())

		reader := NewFragmentReader(readerIterator)
		result := make([]byte, 0, 100)

		n, err := reader.Read(result)
		assert.NoError(t, err)
		assert.Equal(t, "", string(result[:n]))
		assert.Equal(t, 0, n)
	})

	t.Run("several_reads", func(t *testing.T) {
		readerPool := NewReaderPool(catalog)
		readerIterator := NewFragmentReaderIterator(readerPool, descriptor())

		reader := NewFragmentReader(readerIterator)
		result := make([]byte, 10)

		n, err := reader.Read(result)
		assert.NoError(t, err)
		assert.Equal(t, string(text[:10]), string(result[:n]))
		assert.Equal(t, 10, n)

		n, err = reader.Read(result)
		assert.NoError(t, err)
		assert.Equal(t, string(text[10:20]), string(result[:n]))
		assert.Equal(t, 10, n)
	})

	t.Run("read_empty", func(t *testing.T) {
		readerPool := NewReaderPool(catalog)
		readerIterator := NewFragmentReaderIterator(readerPool, descriptor())

		reader := NewFragmentReader(readerIterator)
		result := make([]byte, 0)

		n, err := reader.Read(result)
		assert.NoError(t, err)
		assert.Equal(t, 0, n)
	})

	t.Run("read_already_empty", func(t *testing.T) {
		readerPool := NewReaderPool(catalog)
		readerIterator := NewFragmentReaderIterator(readerPool, descriptor())

		reader := NewFragmentReader(readerIterator)
		result := make([]byte, 100)

		n, err := reader.Read(result)
		assert.NoError(t, err)
		assert.Equal(t, string(text), string(result[:n]))
		assert.Equal(t, 93, n)

		n, err = reader.Read(result)
		assert.ErrorIs(t, err, io.EOF)
		assert.Equal(t, "", string(result[:n]))
		assert.Equal(t, 0, n)
	})

	t.Run("read_after_eof", func(t *testing.T) {
		readerPool := NewReaderPool(catalog)
		readerIterator := NewFragmentReaderIterator(readerPool, descriptor())

		reader := NewFragmentReader(readerIterator)
		result := make([]byte, 80)

		n, err := reader.Read(result)
		assert.NoError(t, err)
		assert.Equal(t, string(text[:80]), string(result[:n]))
		assert.Equal(t, 80, n)

		n, err = reader.Read(result)
		assert.ErrorIs(t, err, io.EOF)
		assert.Equal(t, string(text[80:]), string(result[:n]))
		assert.Equal(t, 13, n)

		n, err = reader.Read(result)
		assert.ErrorIs(t, err, io.EOF)
		assert.Equal(t, "", string(result[:n]))
		assert.Equal(t, 0, n)
	})

	t.Run("reader_error", func(t *testing.T) {
		descriptor := descriptor()
		descriptor.Fragments[0] = Fragment{
			Location: "Unexisting id",
			Length:   10,
		}
		readerPool := NewReaderPool(catalog)
		readerIterator := NewFragmentReaderIterator(readerPool, descriptor)

		reader := NewFragmentReader(readerIterator)
		result := make([]byte, 100)

		n, err := reader.Read(result)
		assert.ErrorIs(t, err, ErrNotFound)
		assert.Equal(t, make([]byte, 100), result)
		assert.Equal(t, 0, n)
	})
}
