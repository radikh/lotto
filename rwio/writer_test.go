package rwio

import (
	"bytes"
	"fmt"
	"io"
	"io/fs"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

type MemoryWriterFactory struct{}

func (m *MemoryWriterFactory) New(id int) io.Writer {
	return &bytes.Buffer{}
}

func TestWriteAndRead(t *testing.T) {
	textDummy := "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed sed nisl nec nisl luctus lacini"
	textDummy = strings.Repeat(textDummy, 100)
	type testcase struct {
		fragmentLength int
		writersNumber  int
		text           string
	}

	testCases := []testcase{
		{
			fragmentLength: 1,
			writersNumber:  1,
			text:           textDummy,
		},
		{
			fragmentLength: 1,
			writersNumber:  100,
			text:           textDummy,
		},
		{
			fragmentLength: 100,
			writersNumber:  1,
			text:           textDummy,
		},
		{
			fragmentLength: 100,
			writersNumber:  100,
			text:           textDummy,
		},
		{
			fragmentLength: 100,
			writersNumber:  1,
			text:           textDummy,
		},
		{
			fragmentLength: 100,
			writersNumber:  2,
			text:           textDummy,
		},
		{
			fragmentLength: 10000,
			writersNumber:  2,
			text:           textDummy,
		},
		{
			fragmentLength: 10000,
			writersNumber:  1,
			text:           textDummy,
		},
		{
			fragmentLength: 100,
			writersNumber:  10000,
			text:           textDummy,
		},
		{
			fragmentLength: 10000,
			writersNumber:  10000,
			text:           textDummy,
		},
		{
			fragmentLength: 10000,
			writersNumber:  10000,
			text:           strings.Repeat(textDummy, 100),
		},
	}

	for i, tc := range testCases {
		name := fmt.Sprintf(
			"%d_len_%d_num_%d_txtlen_%d",
			i,
			tc.fragmentLength,
			tc.writersNumber,
			len(tc.text),
		)

		t.Run(name, func(t *testing.T) {
			tripletext := tc.text + tc.text + tc.text

			catalog := NewInMemoryCatalog()
			descriptor := &FileDescriptor{}

			f := fs.FS(catalog)

			fragmentLength := tc.fragmentLength
			writersNumber := tc.writersNumber

			writer := NewChunkWriter(fragmentLength, writersNumber, catalog, descriptor)

			t.Run("Write", func(t *testing.T) {
				n, err := writer.Write([]byte(tc.text))
				assert.NoError(t, err)
				assert.Equal(t, len(tc.text), n)

				n, err = writer.Write([]byte(tc.text))
				assert.NoError(t, err)
				assert.Equal(t, len(tc.text), n)

				n, err = writer.Write([]byte(tc.text))
				assert.NoError(t, err)
				assert.Equal(t, len(tc.text), n)
			})

			t.Run("Read", func(t *testing.T) {
				readerPool := NewReaderPool(catalog)
				readerIterator := NewFragmentReaderIterator(readerPool, descriptor)

				reader := NewFragmentReader(readerIterator)

				result := make([]byte, len(tripletext))

				n, err := reader.Read(result)
				assert.NoError(t, err)
				assert.Equal(t, tripletext, string(result))
				assert.Equal(t, len(tripletext), n)
			})
		})
	}

	t.Run("error", func(t *testing.T) {
		t.Run("zero_fragment_length", func(t *testing.T) {
			text := textDummy

			catalog := NewInMemoryCatalog()
			descriptor := &FileDescriptor{}

			fragmentLength := 0
			writersNumber := 10

			writer := NewChunkWriter(fragmentLength, writersNumber, catalog, descriptor)

			n, err := writer.Write([]byte(text))
			assert.ErrorIs(t, err, ErrNoSpaceToWrite)
			assert.Equal(t, 0, n)
		})

		t.Run("zero_writers_number", func(t *testing.T) {
			text := textDummy

			catalog := NewInMemoryCatalog()
			descriptor := &FileDescriptor{}

			fragmentLength := 10
			writersNumber := 0

			writer := NewChunkWriter(fragmentLength, writersNumber, catalog, descriptor)

			f := func() {
				writer.Write([]byte(text))
			}

			assert.Panics(t, f)
		})
	})
}
