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
		writer, err := NewRoundRobinChunkWriter(pool, 2, 3)
		assert.NoError(t, err)

		n, err := writer.Write([]byte(text))
		assert.NoError(t, err)
		assert.Equal(t, len(text), n)

		return writer.Descriptor()
	}

	t.Run("read_with_eof", func(t *testing.T) {
		reader := NewFragmentReader(catalog, descriptor())
		result := make([]byte, 80)

		n, err := reader.Read(result)
		assert.NoError(t, err)
		assert.Equal(t, string(text[:80]), string(result))
		assert.Equal(t, 80, n)
	})

	t.Run("result_bigger", func(t *testing.T) {
		reader := NewFragmentReader(catalog, descriptor())
		result := make([]byte, 100)

		n, err := reader.Read(result)
		assert.NoError(t, err)
		assert.Equal(t, string(text), string(result[:n]))
		assert.Equal(t, 93, n)
	})

	t.Run("bigger_cap", func(t *testing.T) {
		reader := NewFragmentReader(catalog, descriptor())
		result := make([]byte, 0, 100)

		n, err := reader.Read(result)
		assert.NoError(t, err)
		assert.Equal(t, "", string(result[:n]))
		assert.Equal(t, 0, n)
	})

	t.Run("several_reads", func(t *testing.T) {
		reader := NewFragmentReader(catalog, descriptor())
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
		reader := NewFragmentReader(catalog, descriptor())
		result := make([]byte, 0)

		n, err := reader.Read(result)
		assert.NoError(t, err)
		assert.Equal(t, 0, n)
	})

	t.Run("read_already_empty", func(t *testing.T) {
		reader := NewFragmentReader(catalog, descriptor())
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
		reader := NewFragmentReader(catalog, descriptor())
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

	t.Run("readed_error", func(t *testing.T) {
		descriptor := descriptor()
		descriptor.Fragments[0] = Fragment{
			Location: "Unexisting id",
		}
		reader := NewFragmentReader(catalog, descriptor)
		result := make([]byte, 100)

		n, err := reader.Read(result)
		assert.ErrorIs(t, err, ErrNotFound)
		assert.Equal(t, make([]byte, 100), result)
		assert.Equal(t, 0, n)
	})
}
