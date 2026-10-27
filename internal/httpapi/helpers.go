package httpapi

import (
	"net/http"
	"strings"
	"sync/atomic"
)

func stringsTrimmedHeader(v string) string {
	return strings.TrimSpace(v)
}

type timeoutWriter struct {
	http.ResponseWriter
	wroteHeader atomic.Bool
}

func (w *timeoutWriter) WriteHeader(status int) {
	w.wroteHeader.Store(true)
	w.ResponseWriter.WriteHeader(status)
}

func (w *timeoutWriter) Write(b []byte) (int, error) {
	w.wroteHeader.Store(true)
	return w.ResponseWriter.Write(b)
}

func (w *timeoutWriter) Flush() {
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		w.wroteHeader.Store(true)
		flusher.Flush()
	}
}
