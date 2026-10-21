package events

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func validEnvelope() *EventEnvelope {
	return &EventEnvelope{
		EventID:       "evt-123",
		EventType:     EventTypePayment,
		SchemaVersion: SchemaVersion,
		OccurredAt:    time.Now().UTC().Truncate(time.Second),
		RunID:         "run-abc",
		Payload:       json.RawMessage(`{"amount_cents":1000,"currency":"USD"}`),
	}
}

func TestValidateAcceptsValidEnvelope(t *testing.T) {
	e := validEnvelope()
	if err := Validate(e); err != nil {
		t.Fatalf("expected valid envelope, got %v", err)
	}
}

func TestValidateRejectsNilEnvelope(t *testing.T) {
	if err := Validate(nil); err == nil {
		t.Fatal("expected error for nil envelope")
	}
}

func TestValidateRejectsMissingEventID(t *testing.T) {
	e := validEnvelope()
	e.EventID = ""
	if err := Validate(e); err == nil {
		t.Fatal("expected error for missing event_id")
	}
}

func TestValidateRejectsMissingRunID(t *testing.T) {
	e := validEnvelope()
	e.RunID = ""
	if err := Validate(e); err == nil {
		t.Fatal("expected error for missing run_id")
	}
}

func TestValidateRejectsUnknownEventType(t *testing.T) {
	e := validEnvelope()
	e.EventType = "not_a_real_type"
	if err := Validate(e); err == nil {
		t.Fatal("expected error for unknown event_type")
	}
}

func TestValidateRejectsUnknownSchemaVersion(t *testing.T) {
	e := validEnvelope()
	e.SchemaVersion = 99
	err := Validate(e)
	if err == nil {
		t.Fatal("expected error for unknown schema_version")
	}
	if !errors.Is(err, errUnsupportedVersion) {
		t.Fatalf("expected errUnsupportedVersion, got %v", err)
	}
}

func TestValidateRejectsMissingPayload(t *testing.T) {
	e := validEnvelope()
	e.Payload = nil
	if err := Validate(e); err == nil {
		t.Fatal("expected error for missing payload")
	}
}

func TestValidateRejectsInvalidPayloadJSON(t *testing.T) {
	e := validEnvelope()
	e.Payload = json.RawMessage(`not-json`)
	if err := Validate(e); err == nil {
		t.Fatal("expected error for invalid payload JSON")
	}
}

func TestValidateRejectsNonObjectPayload(t *testing.T) {
	e := validEnvelope()
	e.Payload = json.RawMessage(`[1,2,3]`)
	if err := Validate(e); err == nil {
		t.Fatal("expected error for non-object payload")
	}
	e.Payload = json.RawMessage(`"string"`)
	if err := Validate(e); err == nil {
		t.Fatal("expected error for scalar payload")
	}
}

func TestValidateRejectsOversizedPayload(t *testing.T) {
	e := validEnvelope()
	big := strings.Repeat("a", MaxPayloadBytes+1)
	e.Payload = json.RawMessage(`{"data":"` + big + `"}`)
	if err := Validate(e); err == nil {
		t.Fatal("expected error for oversized payload")
	}
}

func TestValidateRejectsFutureOccurredAt(t *testing.T) {
	e := validEnvelope()
	e.OccurredAt = time.Now().Add(72 * time.Hour).UTC()
	if err := Validate(e); err == nil {
		t.Fatal("expected error for future occurred_at")
	}
}

func TestValidateRejectsAncientOccurredAt(t *testing.T) {
	e := validEnvelope()
	e.OccurredAt = time.Now().Add(-72 * time.Hour).UTC()
	if err := Validate(e); err == nil {
		t.Fatal("expected error for ancient occurred_at")
	}
}

func TestNormalizeOccurredAtIsDeterministic(t *testing.T) {
	e := validEnvelope()
	n1 := NormalizeOccurredAt(e)
	n2 := NormalizeOccurredAt(e)
	if !n1.OccurredAt.Equal(n2.OccurredAt) {
		t.Fatal("normalize should be deterministic")
	}
	if n1.OccurredAt.Nanosecond() != 0 {
		t.Fatal("occurred_at should be truncated to seconds")
	}
}

func TestStableSerialization(t *testing.T) {
	e := validEnvelope()
	a, err1 := json.Marshal(e)
	b, err2 := json.Marshal(e)
	if err1 != nil || err2 != nil {
		t.Fatalf("marshal: %v %v", err1, err2)
	}
	if string(a) != string(b) {
		t.Fatal("envelope serialization must be stable")
	}
}

func TestEnforceMaxEnvelopeBytes(t *testing.T) {
	e := validEnvelope()
	raw, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := EnforceMaxEnvelopeBytes(raw); err != nil {
		t.Fatalf("expected valid-size envelope to pass, got %v", err)
	}
	oversized := make([]byte, MaxEnvelopeBytes+1)
	if err := EnforceMaxEnvelopeBytes(oversized); err == nil {
		t.Fatal("expected error for oversized envelope")
	}
}

func TestIsEnvelopeError(t *testing.T) {
	e := validEnvelope()
	e.EventID = ""
	err := Validate(e)
	if !IsEnvelopeError(err) {
		t.Fatal("expected IsEnvelopeError to be true")
	}
	if err == nil {
		t.Fatal("expected validation error")
	}
}
