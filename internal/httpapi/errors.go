package httpapi

import (
	"encoding/json"
	"net/http"
)

// WriteError renders the shared JSON error envelope. Message must already be
// safe for clients; internal details must never be passed here.
func WriteError(w http.ResponseWriter, r *http.Request, status int, code, message string, fields []FieldError) {
	requestID := requestIDFor(r)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("X-Request-ID", requestID)
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(ErrorEnvelope{
		Code:      code,
		Message:   message,
		RequestID: requestID,
		Fields:    fields,
	})
}

// WriteJSON renders a successful JSON payload with the request identifier.
func WriteJSON(w http.ResponseWriter, r *http.Request, status int, v any) {
	requestID := requestIDFor(r)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("X-Request-ID", requestID)
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// BadRequest renders invalid_json or invalid_field failures.
func BadRequest(w http.ResponseWriter, r *http.Request, code, message string, fields []FieldError) {
	if code == "" {
		code = CodeInvalidField
	}
	WriteError(w, r, http.StatusBadRequest, code, message, fields)
}

// NotFound renders a stable missing-resource error.
func NotFound(w http.ResponseWriter, r *http.Request, message string) {
	if message == "" {
		message = "The requested resource was not found."
	}
	WriteError(w, r, http.StatusNotFound, CodeNotFound, message, nil)
}

// Conflict renders a stable state-conflict error.
func Conflict(w http.ResponseWriter, r *http.Request, message string) {
	if message == "" {
		message = "The request conflicts with the current state."
	}
	WriteError(w, r, http.StatusConflict, CodeConflict, message, nil)
}

// UnsupportedMediaType renders an unsupported content-type error.
func UnsupportedMediaType(w http.ResponseWriter, r *http.Request, message string) {
	if message == "" {
		message = "Content-Type must be application/json."
	}
	WriteError(w, r, http.StatusUnsupportedMediaType, CodeUnsupportedMediaType, message, nil)
}

// PayloadTooLarge renders an oversized-body error.
func PayloadTooLarge(w http.ResponseWriter, r *http.Request, message string) {
	if message == "" {
		message = "Request body is too large."
	}
	WriteError(w, r, http.StatusRequestEntityTooLarge, CodePayloadTooLarge, message, nil)
}

// MethodNotAllowed renders a stable method error.
func MethodNotAllowed(w http.ResponseWriter, r *http.Request, message string) {
	if message == "" {
		message = "HTTP method is not allowed for this endpoint."
	}
	WriteError(w, r, http.StatusMethodNotAllowed, CodeMethodNotAllowed, message, nil)
}

// RequestTimeout renders a bounded-duration timeout error.
func RequestTimeout(w http.ResponseWriter, r *http.Request, message string) {
	if message == "" {
		message = "The request exceeded its maximum duration."
	}
	WriteError(w, r, http.StatusServiceUnavailable, CodeRequestTimeout, message, nil)
}

// Unauthorized renders a stable authentication error without revealing
// whether a key ID exists or which scope was missing.
func Unauthorized(w http.ResponseWriter, r *http.Request, message string) {
	if message == "" {
		message = "Authentication is required."
	}
	WriteError(w, r, http.StatusUnauthorized, CodeUnauthorized, message, nil)
}

// Forbidden renders a stable authorization error for insufficient scope.
func Forbidden(w http.ResponseWriter, r *http.Request, message string) {
	if message == "" {
		message = "This API key does not have the required scope."
	}
	WriteError(w, r, http.StatusForbidden, CodeForbidden, message, nil)
}

// Internal renders a generic failure without exposing internal details.
// The caller-provided err is intentionally not serialized.
func Internal(w http.ResponseWriter, r *http.Request, err error) {
	_ = err
	WriteError(w, r, http.StatusInternalServerError, CodeInternal, "An internal error occurred.", nil)
}
