package rwio

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

type MemoryWriterFactory struct{}

func (m *MemoryWriterFactory) New(id int) io.Writer {
	return &bytes.Buffer{}
}

func TestWriteAndRead(t *testing.T) {
	text := "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed sed nisl nec nisl luctus lacinia"

	catalog := NewInMemoryCatalog()
	pool := NewWriterPool(catalog)

	writer, err := NewRoundRobinChunkWriter(pool, 2, 3)
	assert.NoError(t, err)

	t.Log(writer.writers)

	n, err := writer.Write([]byte(text))
	assert.NoError(t, err)
	assert.Equal(t, len(text), n)

	reader := NewFragmentReader(catalog, writer.Descriptor())

	result := make([]byte, len(text))

	n, err = reader.Read(result)
	assert.NoError(t, err)
	assert.Equal(t, text, string(result))
	assert.Equal(t, len(text), n)

	for _, writerID := range writer.writers.items {
		t.Logf("%s", writer.pool.items[writerID].(NopCloser).Writer.(*bytes.Buffer).String())
	}

	t.Logf("%s", result)
	t.Logf("%+v", writer.Descriptor())
}
