package rwio

import (
	"errors"
	"io/fs"
	"testing"

	"github.com/psanford/memfs"
	"github.com/stretchr/testify/assert"
)

func TestFilesystem(t *testing.T) {
	t.Run("open", func(t *testing.T) {
		mfs := memfs.New()
		mfs.WriteFile("test", []byte("LoremIpsum"), 0644)

		config := Config{
			Files: map[string]FileDescriptor{
				"test": {
					Fragments: []Fragment{
						{
							Location: "test",
							Length:   10,
						},
					},
				},
			},
			Providers: map[string]fs.FS{
				"test": mfs,
			},
		}

		filesystem := NewFilesystem(config)
		file, err := filesystem.Open("test")
		assert.NoError(t, err)
		assert.IsType(t, &File{}, file)

		t.Run("read", func(t *testing.T) {
			result := make([]byte, 10, 100)
			n, err := file.Read(result)
			assert.NoError(t, err)
			assert.Equal(t, "LoremIpsum", result)
			assert.Equal(t, 10, n)
		})
	})
}

var _ fs.FS = &Lotto{}

type Config struct {
	Files     map[string]FileDescriptor
	Providers map[string]fs.FS
}

type Lotto struct {
	Config
}

func NewFilesystem(config Config) *Lotto {
	return &Lotto{
		Config: config,
	}
}

var ErrBrokenDescriptor = errors.New("broken descriptor")

func (l *Lotto) Open(name string) (fs.File, error) {
	descriptor, ok := l.Files[name]
	if !ok {
		return nil, fs.ErrNotExist
	}

	if descriptor.IsEmpty() {
		return nil, ErrBrokenDescriptor
	}

	file := &File{
		FileDescriptor: descriptor,
		Providers:      l.Providers,
	}

	return file, nil
}

var _ fs.File = &File{}

type File struct {
	FileDescriptor
	Providers map[string]fs.FS
}

func (l *File) Stat() (fs.FileInfo, error) {
	panic("implement me")
}

func (l *File) Read([]byte) (int, error) {
	panic("implement me")
}

func (l *File) Close() error {
	panic("implement me")
}
