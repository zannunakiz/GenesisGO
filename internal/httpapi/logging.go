package httpapi

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/zannunakiz/GenesisGO/internal/observability"
)

// RequestLogger writes one structured JSON record per request without logging
// headers, cookies, bodies, or query strings. Only method, path, status,
// duration, and correlation identifiers are recorded.
func RequestLogger(logger *slog.Logger) func(http.Handler) http.Handler {
	if logger == nil {
		logger = slog.Default()
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			requestID := RequestIDFrom(r.Context())
			// Propagate the same correlation identifier under the shared
			// observability context so worker code and event metadata can
			// reuse it without touching transport headers.
			r = r.WithContext(observability.WithRequestID(r.Context(), requestID))
			captured := &statusWriter{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(captured, r)

			attrs := []slog.Attr{
				slog.String("method", r.Method),
				slog.String("path", r.URL.Path),
				slog.Int("status", captured.status),
				slog.Int64("duration_ms", time.Since(start).Milliseconds()),
			}
			observability.LogRedacted(logger, r.Context(), slog.LevelInfo, "request completed", attrs...)
		})
	}
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(b)
}
