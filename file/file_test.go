package file

import (
	"image"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFileReadWrite(t *testing.T) {
	img := image.NewRGBA(image.Rect(0, 0, 100, 100))

	file := NewImageStegoFile(img)
	data := []byte("Hello, world!")

	t.Run("write", func(t *testing.T) {
		t.Run("WritesAllData", func(t *testing.T) {

			n, err := file.Write(data)
			assert.NoError(t, err)
			assert.Equal(t, len(data), n)
		})
	})

	t.Run("read", func(t *testing.T) {
		t.Run("ReadsAllData", func(t *testing.T) {
			result := make([]byte, len(data))
			n, err := file.Read(result)
			assert.NoError(t, err)
			assert.Equal(t, len(data), n)
			assert.Equal(t, data, result)
		})
	})

	t.Run("not_enough_space", func(t *testing.T) {
		img := image.NewRGBA(image.Rect(0, 0, 1, 1))
		file := NewImageStegoFile(img)

		n, err := file.Write(data)
		assert.Equal(t, ErrNotEnoughSpace, err)
		assert.Equal(t, 0, n)
	})
}
