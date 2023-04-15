package rwio

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

type MemoryWriterFactory struct{}

func (m *MemoryWriterFactory) New(id int) io.Writer {
	return &bytes.Buffer{}
}

func TestWriteAndRead(t *testing.T) {
	text := strings.Repeat("Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed sed nisl nec nisl luctus lacini", 1000)
	tripletext := text + text + text

	catalog := NewInMemoryCatalog()
	pool := NewWriterPool(catalog)

	descriptor := &FileDescriptor{}
	recorder := NewDescriptorRecorder(descriptor)

	recPool := NewWriterFactory(pool, recorder)

	fragmentLength := 300
	writersNumber := 60
	writers := make([]io.Writer, 0)
	for i := 0; i < writersNumber; i++ {
		writers = append(writers, recPool.Get())
	}

	ringIterator := NewRing(writers...)

	iterator := NewLimitWriterIteratorWrapper(ringIterator, fragmentLength)

	writer := NewFragmentWriter(iterator)

	t.Log(writer.iterator)

	n, err := writer.Write([]byte(text))
	assert.NoError(t, err)
	assert.Equal(t, len(text), n)

	n, err = writer.Write([]byte(text))
	assert.NoError(t, err)
	assert.Equal(t, len(text), n)

	n, err = writer.Write([]byte(text))
	assert.NoError(t, err)
	assert.Equal(t, len(text), n)

	readerPool := NewReaderPool(catalog)
	readerIterator := NewFragmentReaderIterator(readerPool, descriptor)

	reader := NewFragmentReader(readerIterator)

	result := make([]byte, len(tripletext))

	n, err = reader.Read(result)
	assert.NoError(t, err)
	assert.Equal(t, tripletext, string(result))
	assert.Equal(t, len(tripletext), n)
}
