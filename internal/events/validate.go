package events

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

// Validate checks an envelope for structural soundness and bounded parameters.
// It returns an EnvelopeError describing the first problem found. It never
// echoes raw input values in error messages.
func Validate(e *EventEnvelope) error {
	if e == nil {
		return &EnvelopeError{Field: "envelope", Reason: "is required", Inner: errMissingField}
	}

	if strings.TrimSpace(e.EventID) == "" {
		return &EnvelopeError{Field: "event_id", Reason: "is required", Inner: errMissingField}
	}
	if !validID(e.EventID) {
		return &EnvelopeError{Field: "event_id", Reason: "must be alphanumeric with dashes or underscores", Inner: errInvalidFormat}
	}
	if utf8.RuneCountInString(e.EventID) > MaxIDLength {
		return &EnvelopeError{Field: "event_id", Reason: fmt.Sprintf("must be at most %d characters", MaxIDLength), Inner: errInvalidFormat}
	}

	if !allowedEventTypes[e.EventType] {
		return &EnvelopeError{Field: "event_type", Reason: "must be one of the supported event types", Inner: errInvalidFormat}
	}

	if !SupportedSchemaVersions[e.SchemaVersion] {
		return &EnvelopeError{Field: "schema_version", Reason: "is not a supported schema version", Inner: errUnsupportedVersion}
	}

	if e.OccurredAt.IsZero() {
		return &EnvelopeError{Field: "occurred_at", Reason: "is required", Inner: errMissingField}
	}
	now := time.Now().UTC()
	if e.OccurredAt.After(now.Add(OccurredAtTolerance)) {
		return &EnvelopeError{Field: "occurred_at", Reason: "must not be more than 48 hours in the future", Inner: errInvalidFormat}
	}
	if e.OccurredAt.Before(now.Add(-OccurredAtTolerance)) {
		return &EnvelopeError{Field: "occurred_at", Reason: "must not be more than 48 hours in the past", Inner: errInvalidFormat}
	}

	if strings.TrimSpace(e.RunID) == "" {
		return &EnvelopeError{Field: "run_id", Reason: "is required", Inner: errMissingField}
	}
	if !validID(e.RunID) {
		return &EnvelopeError{Field: "run_id", Reason: "must be alphanumeric with dashes or underscores", Inner: errInvalidFormat}
	}
	if utf8.RuneCountInString(e.RunID) > MaxIDLength {
		return &EnvelopeError{Field: "run_id", Reason: fmt.Sprintf("must be at most %d characters", MaxIDLength), Inner: errInvalidFormat}
	}

	if len(e.Payload) == 0 {
		return &EnvelopeError{Field: "payload", Reason: "is required", Inner: errMissingField}
	}
	if len(e.Payload) > MaxPayloadBytes {
		return &EnvelopeError{Field: "payload", Reason: fmt.Sprintf("must be at most %d bytes", MaxPayloadBytes), Inner: errPayloadTooLarge}
	}
	if !json.Valid(e.Payload) {
		return &EnvelopeError{Field: "payload", Reason: "must be valid JSON", Inner: errInvalidFormat}
	}
	// Payload must be a JSON object, not a scalar or array.
	var first byte
	for _, b := range e.Payload {
		if b == ' ' || b == '\t' || b == '\n' || b == '\r' {
			continue
		}
		first = b
		break
	}
	if first != '{' {
		return &EnvelopeError{Field: "payload", Reason: "must be a JSON object", Inner: errInvalidFormat}
	}

	return nil
}

// validID checks that an identifier uses only safe characters.
func validID(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_') {
			return false
		}
	}
	return true
}

// EnforceMaxEnvelopeBytes rejects envelopes whose serialized form exceeds the
// limit. Callers should invoke this after serialization, before publishing.
func EnforceMaxEnvelopeBytes(raw []byte) error {
	if len(raw) > MaxEnvelopeBytes {
		return &EnvelopeError{Field: "envelope", Reason: fmt.Sprintf("must be at most %d bytes", MaxEnvelopeBytes), Inner: errEnvelopeTooLarge}
	}
	return nil
}

// NormalizeOccurredAt truncates occurred_at to seconds for stable comparison
// and storage. Returns a copy with the normalized time.
func NormalizeOccurredAt(e *EventEnvelope) *EventEnvelope {
	out := *e
	out.OccurredAt = e.OccurredAt.UTC().Truncate(time.Second)
	return &out
}

// IsDuplicateOf reports whether two envelopes refer to the same logical event
// (same event_id and run_id), for dedup decisions at the boundary.
func (e *EventEnvelope) IsDuplicateOf(other *EventEnvelope) bool {
	return e.EventID == other.EventID && e.RunID == other.RunID
}
