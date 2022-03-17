package airbyte

import (
	"io"
	"sync"
)

type safeWriter struct {
	w  io.WriteCloser
	mu sync.Mutex
}

func newSafeWriteCloser(w io.WriteCloser) io.WriteCloser {
	return &safeWriter{
		w: w,
	}
}

func (sw *safeWriter) Write(p []byte) (int, error) {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	return sw.w.Write(p)
}

func (sw *safeWriter) Close() error {
	defer sw.mu.Unlock()
	return sw.w.Close()
}
