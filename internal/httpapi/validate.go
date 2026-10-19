package httpapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"
	"unicode/utf8"
)

const maxDecodeOverheadBytes = 1024

// Limits configures transport-boundary validation.
type Limits struct {
	// MaxBodyBytes caps request bodies (must be positive).
	MaxBodyBytes int64
	// MaxStringLength caps individual string fields (0 disables the check).
	MaxStringLength int
	// MaxListItems caps decoded JSON array lengths (0 disables the check).
	MaxListItems int
}

func (l Limits) bodyLimit() int64 {
	if l.MaxBodyBytes <= 0 {
		return 1 << 20
	}
	return l.MaxBodyBytes
}

// RequireJSONContentType rejects non-JSON request methods that carry a body.
func RequireJSONContentType(r *http.Request) *ErrorEnvelope {
	if r.Method == http.MethodGet || r.Method == http.MethodHead || r.Method == http.MethodDelete {
		if r.ContentLength == 0 {
			return nil
		}
	}
	contentType := strings.TrimSpace(r.Header.Get("Content-Type"))
	if contentType == "" {
		return &ErrorEnvelope{Code: CodeUnsupportedMediaType, Message: "Content-Type must be application/json."}
	}
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err != nil || !strings.EqualFold(mediaType, "application/json") {
		return &ErrorEnvelope{Code: CodeUnsupportedMediaType, Message: "Content-Type must be application/json."}
	}
	return nil
}

// DecodeJSON validates content type, body size, and JSON shape at the boundary.
// Unknown fields are rejected so malformed or unexpected payloads fail early.
func DecodeJSON[T any](w http.ResponseWriter, r *http.Request, limits Limits, dst *T) bool {
	if problem := RequireJSONContentType(r); problem != nil {
		UnsupportedMediaType(w, r, problem.Message)
		return false
	}
	if r.ContentLength > limits.bodyLimit() {
		PayloadTooLarge(w, r, "")
		return false
	}

	limited := http.MaxBytesReader(w, r.Body, limits.bodyLimit()+maxDecodeOverheadBytes)
	defer func() { _ = r.Body.Close() }()

	raw, err := io.ReadAll(limited)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			PayloadTooLarge(w, r, "")
		} else {
			BadRequest(w, r, CodeInvalidJSON, "Request body could not be read.", nil)
		}
		return false
	}
	if int64(len(raw)) > limits.bodyLimit() {
		PayloadTooLarge(w, r, "")
		return false
	}
	if len(bytes.TrimSpace(raw)) == 0 {
		BadRequest(w, r, CodeInvalidJSON, "Request body must contain valid JSON.", nil)
		return false
	}
	if !utf8.Valid(raw) {
		BadRequest(w, r, CodeInvalidJSON, "Request body must be valid UTF-8 JSON.", nil)
		return false
	}

	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		BadRequest(w, r, CodeInvalidJSON, "Request body must contain valid JSON.", nil)
		return false
	}
	if decoder.More() {
		BadRequest(w, r, CodeInvalidJSON, "Request body must contain a single JSON value.", nil)
		return false
	}
	return true
}

// Validator accumulates safe field problems without echoing raw values.
type Validator struct {
	Fields []FieldError
}

// Invalid reports whether any field problem was recorded.
func (v *Validator) Invalid() bool { return len(v.Fields) > 0 }

// Add records a field problem with a fixed, non-sensitive reason.
func (v *Validator) Add(field, reason string) {
	field = strings.TrimSpace(field)
	if field == "" {
		field = "value"
	}
	if reason == "" {
		reason = "is invalid"
	}
	v.Fields = append(v.Fields, FieldError{Field: field, Reason: reason})
}

// CheckString validates presence and length without returning the value.
func (v *Validator) CheckString(field, value string, maxLength int, required bool) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		if required {
			v.Add(field, "is required")
		}
		return
	}
	if maxLength > 0 && len([]rune(trimmed)) > maxLength {
		v.Add(field, fmt.Sprintf("must be at most %d characters", maxLength))
	}
}

// CheckEnum validates an allowlisted value. Raw input is never echoed.
func (v *Validator) CheckEnum(field, value string, allowed ...string) {
	if value == "" {
		v.Add(field, "is required")
		return
	}
	for _, item := range allowed {
		if value == item {
			return
		}
	}
	v.Add(field, "must be one of the supported values")
}

// CheckListLength bounds JSON array sizes at the boundary.
func (v *Validator) CheckListLength(field string, length, maxItems int) {
	if maxItems > 0 && length > maxItems {
		v.Add(field, fmt.Sprintf("must contain at most %d items", maxItems))
	}
}

// Fail renders accumulated problems in the shared envelope.
func (v *Validator) Fail(w http.ResponseWriter, r *http.Request) {
	BadRequest(w, r, CodeInvalidField, "Request validation failed.", v.Fields)
}
