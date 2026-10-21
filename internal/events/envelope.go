// Package events defines the versioned event envelope used for publishing,
// delivery, and audit across GenesisGo. Envelopes are validated at the
// transport boundary; invalid envelopes are rejected and recorded, never
// silently dropped.
package events

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// SchemaVersion is the current envelope schema version. Only this version is
// accepted; unknown versions are rejected.
const SchemaVersion = 1

// SupportedSchemaVersions is the allowlist of accepted schema versions.
var SupportedSchemaVersions = map[int]bool{SchemaVersion: true}

// MaxEnvelopeBytes caps the total serialized envelope size.
const MaxEnvelopeBytes = 1 << 20 // 1 MiB

// MaxPayloadBytes caps the business payload size within an envelope.
const MaxPayloadBytes = 256 * 1024 // 256 KiB

// MaxIDLength caps event_id, event_type, and run_id lengths.
const MaxIDLength = 128

// OccurredAtTolerance bounds how far in the past or future an occurred_at may be.
const OccurredAtTolerance = 48 * time.Hour

// EventType is a typed business event category.
type EventType string

const (
	EventTypePayment    EventType = "payment"
	EventTypeInventory  EventType = "inventory"
	EventTypeOrder      EventType = "order"
	EventTypeRefund     EventType = "refund"
	EventTypeAdjustment EventType = "adjustment"
)

// allowedEventTypes is the envelope-level allowlist.
var allowedEventTypes = map[EventType]bool{
	EventTypePayment:    true,
	EventTypeInventory:  true,
	EventTypeOrder:      true,
	EventTypeRefund:     true,
	EventTypeAdjustment: true,
}

// EventEnvelope is the versioned contract for every event entering GenesisGo.
// It carries the business payload as raw JSON so the envelope itself remains
// type-agnostic while the payload is validated separately.
type EventEnvelope struct {
	EventID      string          `json:"event_id"`
	EventType    EventType       `json:"event_type"`
	SchemaVersion int            `json:"schema_version"`
	OccurredAt   time.Time       `json:"occurred_at"`
	RunID        string          `json:"run_id"`
	Payload      json.RawMessage `json:"payload"`
}

// EnvelopeError describes why an envelope was rejected, without echoing raw
// input values back to callers.
type EnvelopeError struct {
	Field   string
	Reason  string
	Inner   error
}

func (e *EnvelopeError) Error() string {
	if e.Inner != nil {
		return fmt.Sprintf("envelope field %q: %s: %v", e.Field, e.Reason, e.Inner)
	}
	return fmt.Sprintf("envelope field %q: %s", e.Field, e.Reason)
}

func (e *EnvelopeError) Unwrap() error { return e.Inner }

// IsEnvelopeError reports whether err is an envelope validation error.
func IsEnvelopeError(err error) bool {
	var target *EnvelopeError
	return errors.As(err, &target)
}

// errMissingField is returned when a required envelope field is absent.
var errMissingField = errors.New("required field is missing")

// errInvalidFormat is returned when a field fails format validation.
var errInvalidFormat = errors.New("invalid format")

// errUnsupportedVersion is returned when schema_version is not accepted.
var errUnsupportedVersion = errors.New("unsupported schema version")

// errPayloadTooLarge is returned when the payload exceeds MaxPayloadBytes.
var errPayloadTooLarge = errors.New("payload too large")

// errEnvelopeTooLarge is returned when the full envelope exceeds MaxEnvelopeBytes.
var errEnvelopeTooLarge = errors.New("envelope too large")
