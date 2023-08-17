package file

import (
	"errors"
	"image"
)

type ImageStegoFile struct {
	image *image.RGBA

	readPointer  int
	writePointer int
}

func NewImageStegoFile(image *image.RGBA) *ImageStegoFile {
	return &ImageStegoFile{
		image: image,
	}
}

func (f *ImageStegoFile) Read(buf []byte) (int, error) {
	n := 0
	for i := range buf {
		for j := 0; j < 8; j++ {
			bit := f.image.Pix[f.readPointer] & 1
			buf[i] |= bit << j
			f.readPointer++
		}
		n++
	}

	return n, nil
}

func (f *ImageStegoFile) Write(buf []byte) (int, error) {
	n := 0
	for _, b := range buf {
		if !f.isEnoughSpaceForSymbol() {
			return n, ErrNotEnoughSpace
		}

		for i := 0; i < 8; i++ {
			bit := (b >> i) & 1

			f.image.Pix[f.writePointer] = f.image.Pix[f.writePointer] | bit

			f.writePointer++
		}
		n++
	}

	return n, nil
}

func (f *ImageStegoFile) isEnoughSpaceForSymbol() bool {
	return f.writePointer <= len(f.image.Pix)-1-8
}

var ErrNotEnoughSpace = errors.New("not enough space in image")
