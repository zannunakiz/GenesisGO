package events

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// RejectedEvent records an envelope that failed validation and was rejected.
// It is persisted for audit so invalid events are never silently dropped.
type RejectedEvent struct {
	ID         string     `json:"id"`
	RunID      string     `json:"run_id"`
	EventID    string     `json:"event_id"`
	EventType  string     `json:"event_type"`
	Reason     string     `json:"reason"`
	ReceivedAt time.Time  `json:"received_at"`
	Envelope   []byte     `json:"envelope,omitempty"`
}

// Recorder records rejected events for audit. Implementations must be safe for
// concurrent use.
type Recorder interface {
	Record(ctx context.Context, envelope *EventEnvelope, reason string) error
}

// NoopRecorder discards rejected events. Useful in tests or when rejection
// recording is not configured.
type NoopRecorder struct{}

func (NoopRecorder) Record(ctx context.Context, envelope *EventEnvelope, reason string) error {
	return nil
}

// Rejection wraps an envelope with the reason it was rejected, ready for
// recording.
type Rejection struct {
	Envelope *EventEnvelope
	Reason   string
}

// FormatReason builds a stable, non-sensitive reason string for a rejection.
func FormatReason(field, reason string) string {
	return fmt.Sprintf("field=%s reason=%s", field, reason)
}

// MarshalRejection serializes a Rejection to JSON without echoing raw payload
// values beyond what is already in the envelope.
func MarshalRejection(r Rejection) ([]byte, error) {
	record := struct {
		EventID        string    `json:"event_id"`
		EventType      string    `json:"event_type"`
		SchemaVersion  int       `json:"schema_version"`
		RunID          string    `json:"run_id"`
		OccurredAt     time.Time `json:"occurred_at"`
		Reason         string    `json:"reason"`
		ReceivedAt     time.Time `json:"received_at"`
	}{
		EventID:        r.Envelope.EventID,
		EventType:      string(r.Envelope.EventType),
		SchemaVersion:  r.Envelope.SchemaVersion,
		RunID:          r.Envelope.RunID,
		OccurredAt:     r.Envelope.OccurredAt,
		Reason:         r.Reason,
		ReceivedAt:     time.Now().UTC(),
	}
	return json.Marshal(record)
}
