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
	text := "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed sed nisl nec nisl luctus lacini"
	tripletext := text + text + text

	catalog := NewInMemoryCatalog()
	pool := NewWriterPool(catalog)

	writer, err := NewRoundRobinChunkWriter(pool, 2, 3)
	assert.NoError(t, err)

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

	reader := NewFragmentReader(catalog, writer.Descriptor())

	result := make([]byte, len(tripletext))

	n, err = reader.Read(result)
	assert.NoError(t, err)
	assert.Equal(t, tripletext, string(result))
	assert.Equal(t, len(tripletext), n)

	t.Logf("%s", result)
	t.Logf("%+v", writer.Descriptor())
}
