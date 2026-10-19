package httpapi

import (
	"net/http"

	"github.com/zannunakiz/GenesisGO/internal/protection"
)

// TooManyRequests renders a stable rate-limit error.
func TooManyRequests(w http.ResponseWriter, r *http.Request, message string) {
	if message == "" {
		message = "Rate limit exceeded. Please retry later."
	}
	WriteError(w, r, http.StatusTooManyRequests, CodeTooManyRequests, message, nil)
}

// ServiceUnavailable renders a stable protection-failure error.
func ServiceUnavailable(w http.ResponseWriter, r *http.Request, message string) {
	if message == "" {
		message = "Protection service is temporarily unavailable."
	}
	WriteError(w, r, http.StatusServiceUnavailable, CodeServiceUnavailable, message, nil)
}

// Protection guards abuse-sensitive routes with the optional Arcjet adapter.
// Disabled mode applies only the local fallback limiter. Denials use the
// shared error envelope and never echo key material. Health endpoints must
// not be wrapped.
func Protection(guard *protection.Protector) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if guard == nil {
				Internal(w, r, nil)
				return
			}
			switch guard.Check(r) {
			case protection.Allow:
				next.ServeHTTP(w, r)
			case protection.DenyRateLimit:
				TooManyRequests(w, r, "")
			case protection.DenyForbidden:
				Forbidden(w, r, "This request was blocked by request protection.")
			case protection.DenyUnavailable:
				ServiceUnavailable(w, r, "")
			default:
				Internal(w, r, nil)
			}
		})
	}
}
