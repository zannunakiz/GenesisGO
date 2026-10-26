package events

import (
	"encoding/json"
	"testing"
	"time"
)

// FuzzValidate exercises envelope validation with arbitrary inputs to catch
// panics or unexpected acceptance of malformed envelopes. No test data
// contains secrets.
func FuzzValidate(f *testing.F) {
	seed := validEnvelope()
	seedJSON, _ := json.Marshal(seed)
	f.Add(string(seedJSON))
	f.Add(`{"event_id":"evt-1","event_type":"payment.succeeded","schema_version":1,"occurred_at":"2026-01-01T12:00:00Z","run_id":"run-1","payload":{"amount_cents":100,"currency":"USD"}}`)
	f.Add(`{}`)
	f.Add(`not-json`)
	f.Add(`{"event_id":"","event_type":"payment.succeeded","schema_version":1,"occurred_at":"2026-01-01T12:00:00Z","run_id":"run-1","payload":{}}`)
	f.Add(`{"event_id":"evt-1","event_type":"unknown.type","schema_version":1,"occurred_at":"2026-01-01T12:00:00Z","run_id":"run-1","payload":{}}`)

	f.Fuzz(func(t *testing.T, raw string) {
		var e EventEnvelope
		if err := json.Unmarshal([]byte(raw), &e); err != nil {
			return
		}
		if err := Validate(&e); err != nil {
			return
		}
		if e.EventID == "" || e.RunID == "" {
			t.Fatal("validation passed with missing required ID")
		}
		if !allowedEventTypes[e.EventType] {
			t.Fatal("validation passed with unknown event type")
		}
		if !SupportedSchemaVersions[e.SchemaVersion] {
			t.Fatal("validation passed with unsupported schema version")
		}
		if e.OccurredAt.IsZero() {
			t.Fatal("validation passed with zero occurred_at")
		}
		if len(e.Payload) == 0 {
			t.Fatal("validation passed with empty payload")
		}
	})
}

// FuzzNormalizeOccurredAt ensures normalization never panics and is stable.
func FuzzNormalizeOccurredAt(f *testing.F) {
	f.Add(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC).String())
	f.Add(time.Date(2026, 6, 15, 8, 30, 45, 123456789, time.UTC).String())

	f.Fuzz(func(t *testing.T, raw string) {
		tm, err := time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			return
		}
		e := &EventEnvelope{OccurredAt: tm}
		n1 := NormalizeOccurredAt(e)
		n2 := NormalizeOccurredAt(e)
		if !n1.OccurredAt.Equal(n2.OccurredAt) {
			t.Fatal("normalize not deterministic")
		}
		if n1.OccurredAt.Nanosecond() != 0 {
			t.Fatal("occurred_at not truncated to seconds")
		}
	})
}
