package httpapi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"
)

type contextKey string

const requestIDKey contextKey = "request_id"

// Stable error codes returned in the JSON error envelope.
const (
	CodeInvalidJSON          = "invalid_json"
	CodeInvalidField         = "invalid_field"
	CodeUnsupportedMediaType = "unsupported_media_type"
	CodePayloadTooLarge      = "payload_too_large"
	CodeNotFound             = "not_found"
	CodeConflict             = "conflict"
	CodeMethodNotAllowed     = "method_not_allowed"
	CodeRequestTimeout       = "request_timeout"
	CodeInternal             = "internal_error"
)

// FieldError describes a single request field problem without echoing raw input.
type FieldError struct {
	Field  string `json:"field"`
	Reason string `json:"reason"`
}

// ErrorEnvelope is the shared transport-level error shape.
type ErrorEnvelope struct {
	Code      string       `json:"code"`
	Message   string       `json:"message"`
	RequestID string       `json:"request_id"`
	Fields    []FieldError `json:"fields,omitempty"`
}

// RequestIDFrom returns the request identifier stored in the context, if any.
func RequestIDFrom(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if v, ok := ctx.Value(requestIDKey).(string); ok {
		return v
	}
	return ""
}

// ContextWithRequestID stores a request identifier in the context.
func ContextWithRequestID(ctx context.Context, requestID string) context.Context {
	return context.WithValue(ctx, requestIDKey, requestID)
}

// NewRequestID generates a random correlation identifier for responses.
func NewRequestID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "req-unavailable"
	}
	return hex.EncodeToString(b[:])
}

func requestIDFor(r *http.Request) string {
	if r != nil {
		if id := RequestIDFrom(r.Context()); id != "" {
			return id
		}
	}
	return NewRequestID()
}
