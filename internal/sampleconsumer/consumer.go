// Package sampleconsumer implements a realistic sample payment/inventory
// consumer for GenesisGo. It processes payment.succeeded and inventory events
// in a small synthetic order domain, persists a side-effect ledger, and
// exposes scenario-controlled fault points. The default mode is safe and
// idempotent; an intentionally unsafe demo mode exists only as a named option
// and is never the default.
package sampleconsumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/zannunakiz/GenesisGO/internal/events"
	"github.com/zannunakiz/GenesisGO/internal/store"
)

// Mode selects the consumer's correctness behavior.
type Mode string

const (
	// ModeSafe is the default, idempotent processing path. Replaying a
	// duplicate event does not create extra effects.
	ModeSafe Mode = "safe"
	// ModeUnsafeDemo is an intentionally unsafe named demo fixture that
	// bypasses idempotency to demonstrate an invariant failure. It must never
	// be the default mode.
	ModeUnsafeDemo Mode = "unsafe_demo"
)

// Fault models a scenario-controlled fault point the consumer may inject.
type Fault struct {
	// Type is the fault to inject. Supported: "crash_after_commit".
	Type string
	// EventID gates the fault to a specific event; empty means any event.
	EventID string
}

// Payload is the synthetic business payload carried by a consumer event.
type Payload struct {
	Type           string  `json:"type"` // payment.succeeded, inventory.adjusted
	AggregateID    string  `json:"aggregate_id"`
	AmountCents    *int64  `json:"amount_cents,omitempty"`
	Currency       string  `json:"currency"`
	IdempotencyKey *string `json:"idempotency_key,omitempty"`
}

// EffectKind maps an event type to the side-effect ledger effect type.
func EffectKind(pt string) string {
	switch pt {
	case "payment.succeeded":
		return "payment"
	case "inventory.adjusted":
		return "inventory"
	default:
		return "unknown"
	}
}

// Ledger is the side-effect persistence interface the consumer depends on.
// *store.Store satisfies it, and tests can provide a fake.
type Ledger interface {
	ListSideEffectRecordsByRun(ctx context.Context, runID string) ([]*store.SideEffectRecord, error)
	CreateSideEffectRecord(ctx context.Context, runID, effectType, aggregateID string, idempotencyKey *string, amountCents *int64, currency string) (*store.SideEffectRecord, error)
}

// Config holds consumer dependencies and behavior.
type Config struct {
	Ledger Ledger
	Mode   Mode
	Fault  *Fault
}

// Consumer processes events against a side-effect ledger.
type Consumer struct {
	ledger Ledger
	mode   Mode
	fault  *Fault
}

// New builds a Consumer from Config. Mode defaults to ModeSafe when empty.
func New(cfg Config) (*Consumer, error) {
	if cfg.Ledger == nil {
		return nil, errors.New("ledger is required")
	}
	mode := cfg.Mode
	if mode == "" {
		mode = ModeSafe
	}
	switch mode {
	case ModeSafe, ModeUnsafeDemo:
	default:
		return nil, fmt.Errorf("unsupported consumer mode %q", mode)
	}
	return &Consumer{ledger: cfg.Ledger, mode: mode, fault: cfg.Fault}, nil
}

// Mode returns the consumer's operating mode.
func (c *Consumer) Mode() Mode { return c.mode }

// ProcessEvent handles a single validated event envelope. It is idempotent in
// safe mode: an event whose idempotency key has already been applied to the
// run is skipped. In unsafe_demo mode idempotency is bypassed, intentionally
// creating duplicate effects to demonstrate a failure.
//
// It returns a nil error when the event was applied or safely skipped, and a
// non-nil error when the consumer could not process the event (for example,
// when a crash_after_commit fault is configured).
func (c *Consumer) ProcessEvent(ctx context.Context, env *events.EventEnvelope) (*store.SideEffectRecord, error) {
	if env == nil {
		return nil, errors.New("envelope is required")
	}
	if err := events.Validate(env); err != nil {
		return nil, err
	}

	payload, err := parsePayload(env.Payload)
	if err != nil {
		return nil, err
	}
	if payload.AggregateID == "" {
		return nil, errors.New("payload aggregate_id is required")
	}

	effectKind := EffectKind(payload.Type)
	if effectKind == "unknown" {
		return nil, fmt.Errorf("unsupported event type %q", payload.Type)
	}

	// Idempotency check: skip if this run already applied the same
	// idempotency key. Unsafe demo mode intentionally bypasses this.
	idemKey := payload.IdempotencyKey
	if c.mode == ModeSafe && idemKey != nil && *idemKey != "" {
		existing, err := c.ledger.ListSideEffectRecordsByRun(ctx, env.RunID)
		if err != nil {
			return nil, fmt.Errorf("list side effects: %w", err)
		}
		for _, eff := range existing {
			if eff.IdempotencyKey != nil && *eff.IdempotencyKey == *idemKey &&
				eff.EffectType == effectKind && eff.AggregateID == payload.AggregateID {
				// Already processed; skip without creating a duplicate.
				return eff, nil
			}
		}
	}

	// Scenario-controlled fault: crash after committing the effect but before
	// acknowledging. The effect is persisted, then an error is returned to
	// simulate the consumer crashing before ack.
	if c.fault != nil && c.fault.Type == "crash_after_commit" &&
		(c.fault.EventID == "" || c.fault.EventID == env.EventID) {
		record, err := c.createEffect(ctx, env, payload, effectKind)
		if err != nil {
			return nil, err
		}
		return record, fmt.Errorf("consumer crashed after commit before ack: %w", errContextCrashed)
	}

	return c.createEffect(ctx, env, payload, effectKind)
}

// createEffect persists a single side-effect record for the run.
func (c *Consumer) createEffect(
	ctx context.Context,
	env *events.EventEnvelope,
	payload *Payload,
	effectKind string,
) (*store.SideEffectRecord, error) {
	record, err := c.ledger.CreateSideEffectRecord(
		ctx,
		env.RunID,
		effectKind,
		payload.AggregateID,
		payload.IdempotencyKey,
		payload.AmountCents,
		payload.Currency,
	)
	if err != nil {
		return nil, fmt.Errorf("create side effect: %w", err)
	}
	return record, nil
}

// parsePayload decodes the synthetic business payload.
func parsePayload(raw []byte) (*Payload, error) {
	if len(raw) == 0 {
		return nil, errors.New("payload is required")
	}
	var p Payload
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, fmt.Errorf("invalid payload: %w", err)
	}
	if p.Type == "" {
		return nil, errors.New("payload type is required")
	}
	if !validEventType(p.Type) {
		return nil, fmt.Errorf("unsupported payload type %q", p.Type)
	}
	return &p, nil
}

func validEventType(t string) bool {
	switch t {
	case "payment.succeeded", "inventory.adjusted":
		return true
	}
	return false
}

// errContextCrashed marks a crash_after_commit fault. Callers can use
// errors.Is to distinguish a simulated crash from a real failure.
var errContextCrashed = errors.New("consumer crashed after commit before ack")

// IsCrashAfterCommit reports whether err is a simulated crash_after_commit
// fault rather than a real processing failure.
func IsCrashAfterCommit(err error) bool {
	return errors.Is(err, errContextCrashed)
}
// Compile-time check that *store.Store satisfies Ledger.
var _ Ledger = (*store.Store)(nil)


