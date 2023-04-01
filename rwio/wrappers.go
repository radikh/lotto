package rwio

import "io"

// NopCloser wraps an io.Writer and returns a no-op Closer.
type NopCloser struct {
	io.Writer
}

// NopCloserWriter returns a new NopCloser that wraps the provided io.Writer.
func NopCloserWriter(w io.Writer) io.WriteCloser {
	return NopCloser{w}
}

// Close is a no-op method that always returns nil for NopCloser.
func (NopCloser) Close() error {
	return nil
}

// ErrorReadWriteCloser is a type that implements io.Reader io.Writer and io.Closer,
// and returns a specified error on every Write or Read call.
type ErrorReadWriteCloser struct {
	Err error
}

func NewErrorReadWriteCloser(err error) *ErrorReadWriteCloser {
	return &ErrorReadWriteCloser{Err: err}
}

// Write returns the error stored.
func (w *ErrorReadWriteCloser) Write(p []byte) (int, error) {
	return 0, w.Err
}

// Read returns the error stored.
func (w *ErrorReadWriteCloser) Read(p []byte) (int, error) {
	return 0, w.Err
}

// Close returns the error stored.
func (w *ErrorReadWriteCloser) Close() error {
	return w.Err
}
