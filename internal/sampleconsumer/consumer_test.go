package sampleconsumer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/zannunakiz/GenesisGO/internal/events"
	"github.com/zannunakiz/GenesisGO/internal/store"
)

// fakeLedger records side effects in memory for tests.
type fakeLedger struct {
	effects []*store.SideEffectRecord
}

func (f *fakeLedger) ListSideEffectRecordsByRun(ctx context.Context, runID string) ([]*store.SideEffectRecord, error) {
	var out []*store.SideEffectRecord
	for _, e := range f.effects {
		if e.RunID == runID {
			out = append(out, e)
		}
	}
	return out, nil
}

func (f *fakeLedger) CreateSideEffectRecord(ctx context.Context, runID, effectType, aggregateID string, idempotencyKey *string, amountCents *int64, currency string) (*store.SideEffectRecord, error) {
	rec := &store.SideEffectRecord{
		ID:             "fx-" + aggregateID,
		RunID:          runID,
		EffectType:     effectType,
		AggregateID:    aggregateID,
		IdempotencyKey: idempotencyKey,
		AmountCents:    amountCents,
		Currency:       currency,
		RecordedAt:     time.Now().UTC(),
	}
	f.effects = append(f.effects, rec)
	return rec, nil
}

func env(runID, eventID string, payload []byte) *events.EventEnvelope {
	return &events.EventEnvelope{
		EventID:       eventID,
		EventType:     events.EventTypePayment,
		SchemaVersion: events.SchemaVersion,
		OccurredAt:    time.Now().UTC(),
		RunID:         runID,
		Payload:       payload,
	}
}

func paymentPayload(aggregate, idem string, amount int64) []byte {
	p := `{"type":"payment.succeeded","aggregate_id":"` + aggregate + `","amount_cents":` + itoa(amount) + `,"currency":"USD","idempotency_key":"` + idem + `"}`
	return []byte(p)
}

func inventoryPayload(aggregate, idem string, amount int64) []byte {
	p := `{"type":"inventory.adjusted","aggregate_id":"` + aggregate + `","amount_cents":` + itoa(amount) + `,"currency":"USD","idempotency_key":"` + idem + `"}`
	return []byte(p)
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	if neg {
		b = append([]byte{'-'}, b...)
	}
	return string(b)
}

func countEffects(f *fakeLedger, runID string) int {
	n := 0
	for _, e := range f.effects {
		if e.RunID == runID {
			n++
		}
	}
	return n
}

// ---------------------------------------------------------------------------
// New
// ---------------------------------------------------------------------------

func TestNewRejectsNilLedger(t *testing.T) {
	_, err := New(Config{})
	if err == nil {
		t.Fatal("expected error for nil ledger")
	}
}

func TestNewRejectsUnsupportedMode(t *testing.T) {
	_, err := New(Config{Ledger: &fakeLedger{}, Mode: "bogus"})
	if err == nil {
		t.Fatal("expected error for unsupported mode")
	}
}

func TestNewDefaultsToSafeMode(t *testing.T) {
	c, err := New(Config{Ledger: &fakeLedger{}})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if c.Mode() != ModeSafe {
		t.Fatalf("expected safe default mode, got %s", c.Mode())
	}
}

func TestSafeModeNormalProcessingCreatesEffect(t *testing.T) {
	ledger := &fakeLedger{}
	c, err := New(Config{Ledger: ledger, Mode: ModeSafe})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	rec, err := c.ProcessEvent(context.Background(), env("run-1", "evt-1", paymentPayload("order-100", "pay-100", 1000)))
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	if rec == nil {
		t.Fatal("expected a side effect record")
	}
	if countEffects(ledger, "run-1") != 1 {
		t.Fatalf("expected 1 effect, got %d", countEffects(ledger, "run-1"))
	}
}

func TestSafeModeDuplicateReplayNoExtraEffect(t *testing.T) {
	ledger := &fakeLedger{}
	c, err := New(Config{Ledger: ledger, Mode: ModeSafe})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	rec1, err := c.ProcessEvent(context.Background(), env("run-1", "evt-1", paymentPayload("order-100", "pay-100", 1000)))
	if err != nil {
		t.Fatalf("first process: %v", err)
	}
	rec2, err := c.ProcessEvent(context.Background(), env("run-1", "evt-1", paymentPayload("order-100", "pay-100", 1000)))
	if err != nil {
		t.Fatalf("replay process: %v", err)
	}
	if rec1.ID != rec2.ID {
		t.Fatal("expected the same existing record to be returned on replay")
	}
	if countEffects(ledger, "run-1") != 1 {
		t.Fatalf("expected 1 effect after duplicate replay, got %d", countEffects(ledger, "run-1"))
	}
}

func TestSafeModeDifferentIdemKeysCreateSeparateEffects(t *testing.T) {
	ledger := &fakeLedger{}
	c, err := New(Config{Ledger: ledger, Mode: ModeSafe})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	_, err = c.ProcessEvent(context.Background(), env("run-1", "evt-1", paymentPayload("order-100", "pay-100", 1000)))
	if err != nil {
		t.Fatalf("first process: %v", err)
	}
	_, err = c.ProcessEvent(context.Background(), env("run-1", "evt-2", paymentPayload("order-200", "pay-200", 2000)))
	if err != nil {
		t.Fatalf("second process: %v", err)
	}
	if countEffects(ledger, "run-1") != 2 {
		t.Fatalf("expected 2 effects for different idempotency keys, got %d", countEffects(ledger, "run-1"))
	}
}

// ---------------------------------------------------------------------------
// Unsafe demo mode
// ---------------------------------------------------------------------------

func TestUnsafeDemoModeReplayCreatesExtraEffect(t *testing.T) {
	ledger := &fakeLedger{}
	c, err := New(Config{Ledger: ledger, Mode: ModeUnsafeDemo})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	_, err = c.ProcessEvent(context.Background(), env("run-1", "evt-1", paymentPayload("order-100", "pay-100", 1000)))
	if err != nil {
		t.Fatalf("first process: %v", err)
	}
	_, err = c.ProcessEvent(context.Background(), env("run-1", "evt-1", paymentPayload("order-100", "pay-100", 1000)))
	if err != nil {
		t.Fatalf("replay process: %v", err)
	}
	if countEffects(ledger, "run-1") != 2 {
		t.Fatalf("expected 2 effects (duplicate) in unsafe mode, got %d", countEffects(ledger, "run-1"))
	}
}

// ---------------------------------------------------------------------------
// Faults
// ---------------------------------------------------------------------------

func TestCrashAfterCommitPersistsThenErrors(t *testing.T) {
	ledger := &fakeLedger{}
	c, err := New(Config{Ledger: ledger, Mode: ModeSafe, Fault: &Fault{Type: "crash_after_commit"}})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	rec, err := c.ProcessEvent(context.Background(), env("run-1", "evt-1", paymentPayload("order-100", "pay-100", 1000)))
	if err == nil {
		t.Fatal("expected crash_after_commit error")
	}
	if !IsCrashAfterCommit(err) {
		t.Fatalf("expected IsCrashAfterCommit to be true, got %v", err)
	}
	if rec == nil {
		t.Fatal("expected the effect to be persisted even though the consumer crashed")
	}
	if countEffects(ledger, "run-1") != 1 {
		t.Fatalf("expected 1 persisted effect after crash, got %d", countEffects(ledger, "run-1"))
	}
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

func TestProcessRejectsNilEnvelope(t *testing.T) {
	c, err := New(Config{Ledger: &fakeLedger{}})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	_, err = c.ProcessEvent(context.Background(), nil)
	if err == nil {
		t.Fatal("expected error for nil envelope")
	}
}

func TestProcessRejectsMissingRunID(t *testing.T) {
	c, err := New(Config{Ledger: &fakeLedger{}})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	bad := env("", "evt-1", paymentPayload("order-100", "pay-100", 1000))
	bad.RunID = ""
	_, err = c.ProcessEvent(context.Background(), bad)
	if err == nil {
		t.Fatal("expected error for missing run_id")
	}
}

func TestProcessRejectsInvalidPayload(t *testing.T) {
	c, err := New(Config{Ledger: &fakeLedger{}})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	bad := env("run-1", "evt-1", []byte("not-json"))
	_, err = c.ProcessEvent(context.Background(), bad)
	if err == nil {
		t.Fatal("expected error for invalid payload")
	}
}

func TestProcessRejectsUnsupportedEventType(t *testing.T) {
	c, err := New(Config{Ledger: &fakeLedger{}})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	bad := env("run-1", "evt-1", []byte(`{"type":"unknown.event","aggregate_id":"order-1"}`))
	_, err = c.ProcessEvent(context.Background(), bad)
	if err == nil {
		t.Fatal("expected error for unsupported event type")
	}
}

// Ensure events and errors packages are referenced.
var _ = events.EventTypePayment
var _ = errors.New
