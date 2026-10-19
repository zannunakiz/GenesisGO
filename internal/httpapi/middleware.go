package httpapi

import (
	"context"
	"net/http"
	"time"
)

// ServerTimeouts bounds transport-level request handling. Zero values are
// replaced with safe defaults by WithTimeouts.
type ServerTimeouts struct {
	ReadTimeout       time.Duration
	ReadHeaderTimeout time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
	HandlerTimeout    time.Duration
}

// DefaultTimeouts keeps requests bounded while allowing local verification work.
func DefaultTimeouts() ServerTimeouts {
	return ServerTimeouts{
		ReadTimeout:       10 * time.Second,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      20 * time.Second,
		IdleTimeout:       60 * time.Second,
		HandlerTimeout:    15 * time.Second,
	}
}

// WithTimeouts applies transport timeouts to the HTTP server.
func WithTimeouts(srv *http.Server, timeouts ServerTimeouts) *http.Server {
	defaults := DefaultTimeouts()
	if timeouts.ReadTimeout > 0 {
		srv.ReadTimeout = timeouts.ReadTimeout
	} else if srv.ReadTimeout <= 0 {
		srv.ReadTimeout = defaults.ReadTimeout
	}
	if timeouts.ReadHeaderTimeout > 0 {
		srv.ReadHeaderTimeout = timeouts.ReadHeaderTimeout
	} else if srv.ReadHeaderTimeout <= 0 {
		srv.ReadHeaderTimeout = defaults.ReadHeaderTimeout
	}
	if timeouts.WriteTimeout > 0 {
		srv.WriteTimeout = timeouts.WriteTimeout
	} else if srv.WriteTimeout <= 0 {
		srv.WriteTimeout = defaults.WriteTimeout
	}
	if timeouts.IdleTimeout > 0 {
		srv.IdleTimeout = timeouts.IdleTimeout
	} else if srv.IdleTimeout <= 0 {
		srv.IdleTimeout = defaults.IdleTimeout
	}
	return srv
}

// HandlerTimeoutOrDefault returns the effective maximum request duration.
func (t ServerTimeouts) HandlerTimeoutOrDefault() time.Duration {
	if t.HandlerTimeout > 0 {
		return t.HandlerTimeout
	}
	return DefaultTimeouts().HandlerTimeout
}

// RequestID ensures every response carries a correlation identifier and makes
// it available to handlers through the request context.
func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := stringsTrimmedHeader(r.Header.Get("X-Request-ID"))
		if requestID == "" {
			requestID = NewRequestID()
		}
		if len(requestID) > 128 {
			requestID = requestID[:128]
		}
		r = r.WithContext(ContextWithRequestID(r.Context(), requestID))
		w.Header().Set("X-Request-ID", requestID)
		next.ServeHTTP(w, r)
	})
}

// Timeout bounds handler execution with a context deadline. When the deadline
// expires before the handler writes a response, it renders the shared
// request_timeout envelope. Handlers must respect ctx cancellation and avoid
// writing after the deadline.
func Timeout(timeout time.Duration) func(http.Handler) http.Handler {
	if timeout <= 0 {
		timeout = DefaultTimeouts().HandlerTimeoutOrDefault()
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx, cancel := context.WithTimeout(r.Context(), timeout)
			defer cancel()

			tw := &timeoutWriter{ResponseWriter: w}
			next.ServeHTTP(tw, r.WithContext(ctx))

			if ctx.Err() == context.DeadlineExceeded && !tw.wroteHeader.Load() {
				RequestTimeout(w, r, "")
			}
		})
	}
}

// Recover converts panics into a generic internal error without leaking details.
func Recover(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tw := &timeoutWriter{ResponseWriter: w}
		defer func() {
			if rec := recover(); rec != nil {
				_ = rec
				if !tw.wroteHeader.Load() && r.Context().Err() != context.DeadlineExceeded {
					Internal(tw, r, nil)
				}
			}
		}()
		next.ServeHTTP(tw, r)
	})
}
