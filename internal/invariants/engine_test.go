package invariants

import (
	"context"
	"testing"
	"time"
)

func effect(id, runID, effType, agg string, amount *int64, idem *string, recorded time.Time) EffectRecord {
	return EffectRecord{
		ID:             id,
		RunID:          runID,
		EffectType:     effType,
		AggregateID:    agg,
		IdempotencyKey: idem,
		AmountCents:    amount,
		Currency:       "USD",
		RecordedAt:     recorded,
	}
}

func baseTime() time.Time {
	return time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
}

func ptrInt64(v int64) *int64 { return &v }

func ptrString(v string) *string { return &v }

func resultByKey(results []*Result, key string) *Result {
	for _, r := range results {
		if r.InvariantKey == key {
			return r
		}
	}
	return nil
}

func TestNoDuplicateEffectPass(t *testing.T) {
	eng := NewEngine()
	for _, r := range DefaultRules() {
		eng.Register(r)
	}
	ev := Evidence{
		RunID: "run-1",
		Effects: []EffectRecord{
			effect("e1", "run-1", "payment", "order-100", ptrInt64(1000), ptrString("pay-100"), baseTime()),
			effect("e2", "run-1", "payment", "order-200", ptrInt64(2000), ptrString("pay-200"), baseTime()),
		},
	}
	results := eng.EvaluateAll(context.Background(), ev)
	if len(results) != 4 {
		t.Fatalf("expected 4 results, got %d", len(results))
	}
	res := resultByKey(results, RuleNoDuplicateEffect)
	if res.Status != StatusPass {
		t.Fatalf("expected pass, got %s: %s", res.Status, res.Actual)
	}
}

func TestNoDuplicateEffectFail(t *testing.T) {
	eng := NewEngine()
	eng.Register(noDuplicateEffectRule{})
	t1 := baseTime()
	ev := Evidence{
		RunID: "run-1",
		Effects: []EffectRecord{
			effect("e1", "run-1", "payment", "order-100", ptrInt64(1000), ptrString("pay-100"), t1),
			effect("e2", "run-1", "payment", "order-100", ptrInt64(1000), ptrString("pay-100"), t1.Add(time.Second)),
		},
	}
	res := resultByKey(eng.EvaluateAll(context.Background(), ev), RuleNoDuplicateEffect)
	if res.Status != StatusFail {
		t.Fatalf("expected fail, got %s", res.Status)
	}
	if len(res.Evidence) != 2 {
		t.Fatalf("expected 2 evidence IDs, got %d: %v", len(res.Evidence), res.Evidence)
	}
	if res.Expected == "" || res.Actual == "" {
		t.Fatal("expected and actual must be populated")
	}
}
func TestNoNegativeInventoryPass(t *testing.T) {
	eng := NewEngine()
	eng.Register(noNegativeInventoryRule{})
	a100 := int64(100)
	m50 := int64(-50)
	ev := Evidence{
		RunID: "run-1",
		Effects: []EffectRecord{
			effect("e1", "run-1", "inventory", "sku-1", &a100, nil, baseTime()),
			effect("e2", "run-1", "inventory", "sku-1", &m50, nil, baseTime().Add(time.Second)),
		},
	}
	res := resultByKey(eng.EvaluateAll(context.Background(), ev), RuleNoNegativeInventory)
	if res.Status != StatusPass {
		t.Fatalf("expected pass, got %s: %s", res.Status, res.Actual)
	}
}

func TestNoNegativeInventoryFail(t *testing.T) {
	eng := NewEngine()
	eng.Register(noNegativeInventoryRule{})
	a10 := int64(10)
	m20 := int64(-20)
	ev := Evidence{
		RunID: "run-1",
		Effects: []EffectRecord{
			effect("e1", "run-1", "inventory", "sku-1", &a10, nil, baseTime()),
			effect("e2", "run-1", "inventory", "sku-1", &m20, nil, baseTime().Add(time.Second)),
		},
	}
	res := resultByKey(eng.EvaluateAll(context.Background(), ev), RuleNoNegativeInventory)
	if res.Status != StatusFail {
		t.Fatalf("expected fail, got %s", res.Status)
	}
}

func TestIdempotencyKeyUniquePass(t *testing.T) {
	eng := NewEngine()
	eng.Register(idempotencyKeyUniqueRule{})
	ev := Evidence{
		RunID: "run-1",
		Effects: []EffectRecord{
			effect("e1", "run-1", "payment", "order-1", ptrInt64(100), ptrString("key-1"), baseTime()),
			effect("e2", "run-1", "payment", "order-2", ptrInt64(200), ptrString("key-2"), baseTime()),
		},
	}
	res := resultByKey(eng.EvaluateAll(context.Background(), ev), RuleIdempotencyKeyUnique)
	if res.Status != StatusPass {
		t.Fatalf("expected pass, got %s", res.Status)
	}
}

func TestIdempotencyKeyUniqueFail(t *testing.T) {
	eng := NewEngine()
	eng.Register(idempotencyKeyUniqueRule{})
	t1 := baseTime()
	ev := Evidence{
		RunID: "run-1",
		Effects: []EffectRecord{
			effect("e1", "run-1", "payment", "order-1", ptrInt64(100), ptrString("key-1"), t1),
			effect("e2", "run-1", "payment", "order-1", ptrInt64(100), ptrString("key-1"), t1.Add(time.Second)),
		},
	}
	res := resultByKey(eng.EvaluateAll(context.Background(), ev), RuleIdempotencyKeyUnique)
	if res.Status != StatusFail {
		t.Fatalf("expected fail, got %s", res.Status)
	}
}

func TestAllowedOrderTransitionPass(t *testing.T) {
	eng := NewEngine()
	eng.Register(allowedOrderTransitionRule{})
	a100 := int64(100)
	m100 := int64(-100)
	ev := Evidence{
		RunID: "run-1",
		Effects: []EffectRecord{
			effect("e1", "run-1", "order", "order-1", &a100, nil, baseTime()),
			effect("e2", "run-1", "order", "order-1", &m100, nil, baseTime().Add(time.Second)),
		},
	}
	res := resultByKey(eng.EvaluateAll(context.Background(), ev), RuleAllowedOrderTransition)
	if res.Status != StatusPass {
		t.Fatalf("expected pass, got %s", res.Status)
	}
}

func TestAllowedOrderTransitionFail(t *testing.T) {
	eng := NewEngine()
	eng.Register(allowedOrderTransitionRule{})
	a100 := int64(100)
	m100 := int64(-100)
	a200 := int64(200)
	t1 := baseTime()
	ev := Evidence{
		RunID: "run-1",
		Effects: []EffectRecord{
			effect("e1", "run-1", "order", "order-1", &a100, nil, t1),
			effect("e2", "run-1", "order", "order-1", &m100, nil, t1.Add(time.Second)),
			effect("e3", "run-1", "order", "order-1", &a200, nil, t1.Add(2*time.Second)),
		},
	}
	res := resultByKey(eng.EvaluateAll(context.Background(), ev), RuleAllowedOrderTransition)
	if res.Status != StatusFail {
		t.Fatalf("expected fail, got %s", res.Status)
	}
}

func TestResultsStableAcrossRuns(t *testing.T) {
	eng := NewEngine()
	for _, r := range DefaultRules() {
		eng.Register(r)
	}
	a100 := int64(100)
	m50 := int64(-50)
	ev := Evidence{
		RunID: "run-1",
		Effects: []EffectRecord{
			effect("e1", "run-1", "payment", "order-1", &a100, ptrString("k1"), baseTime()),
			effect("e2", "run-1", "inventory", "sku-1", &m50, nil, baseTime().Add(time.Second)),
		},
	}
	first := eng.EvaluateAll(context.Background(), ev)
	second := eng.EvaluateAll(context.Background(), ev)
	if len(first) != len(second) {
		t.Fatal("result count changed across runs")
	}
	for i := range first {
		if first[i].InvariantKey != second[i].InvariantKey {
			t.Fatal("invariant key order changed across runs")
		}
		if first[i].Status != second[i].Status {
			t.Fatalf("status changed for %s across runs", first[i].InvariantKey)
		}
		if first[i].Actual != second[i].Actual {
			t.Fatalf("actual changed for %s across runs", first[i].InvariantKey)
		}
	}
}

// Table-driven ordering test: sequence of effects determines whether the
// allowed_order_transition invariant passes or fails.
func TestAllowedOrderTransitionTableDriven(t *testing.T) {
	a100 := int64(100)
	m100 := int64(-100)
	a200 := int64(200)

	cases := []struct {
		name    string
		effects []EffectRecord
		want    Status
	}{
		{
			name: "create then fulfill is valid",
			effects: []EffectRecord{
				effect("e1", "run-1", "order", "order-1", &a100, nil, baseTime()),
				effect("e2", "run-1", "order", "order-1", &m100, nil, baseTime().Add(time.Second)),
			},
			want: StatusPass,
		},
		{
			name: "create then cancel is valid",
			effects: []EffectRecord{
				effect("e1", "run-1", "order", "order-1", &a100, nil, baseTime()),
				effect("e2", "run-1", "order", "order-1", &m100, nil, baseTime().Add(time.Second)),
			},
			want: StatusPass,
		},
		{
			name: "fulfill before create is invalid",
			effects: []EffectRecord{
				effect("e1", "run-1", "order", "order-1", &m100, nil, baseTime()),
				effect("e2", "run-1", "order", "order-1", &a100, nil, baseTime().Add(time.Second)),
			},
			want: StatusFail,
		},
		{
			name: "cancel then create is invalid",
			effects: []EffectRecord{
				effect("e1", "run-1", "order", "order-1", &m100, nil, baseTime()),
				effect("e2", "run-1", "order", "order-1", &a200, nil, baseTime().Add(time.Second)),
			},
			want: StatusFail,
		},
		{
			name: "create then create is valid (no non-positive yet)",
			effects: []EffectRecord{
				effect("e1", "run-1", "order", "order-1", &a100, nil, baseTime()),
				effect("e2", "run-1", "order", "order-1", &a200, nil, baseTime().Add(time.Second)),
			},
			want: StatusPass,
		},
		{
			name: "single create is valid",
			effects: []EffectRecord{
				effect("e1", "run-1", "order", "order-1", &a100, nil, baseTime()),
			},
			want: StatusPass,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			eng := NewEngine()
			eng.Register(allowedOrderTransitionRule{})
			ev := Evidence{RunID: "run-1", Effects: tc.effects}
			res := resultByKey(eng.EvaluateAll(context.Background(), ev), RuleAllowedOrderTransition)
			if res.Status != tc.want {
				t.Fatalf("expected %s, got %s: %s", tc.want, res.Status, res.Actual)
			}
		})
	}
}

// TestSafeConsumerSatisfiesAllInvariants simulates a safe consumer producing
// idempotent effects and verifies all four invariants pass.
func TestSafeConsumerSatisfiesAllInvariants(t *testing.T) {
	eng := NewEngine()
	for _, r := range DefaultRules() {
		eng.Register(r)
	}
	a100 := int64(100)
	ev := Evidence{
		RunID: "run-safe",
		Effects: []EffectRecord{
			effect("e1", "run-safe", "payment", "order-1", &a100, ptrString("pay-1"), baseTime()),
			// Duplicate replay in safe mode: skipped, no second effect.
			effect("e2", "run-safe", "inventory", "sku-1", &a100, nil, baseTime().Add(time.Second)),
		},
	}
	results := eng.EvaluateAll(context.Background(), ev)
	for _, res := range results {
		if res.Status != StatusPass {
			t.Fatalf("safe consumer: invariant %s unexpectedly %s: %s", res.InvariantKey, res.Status, res.Actual)
		}
	}
}

// TestUnsafeConsumerTriggersExpectedInvariantFailure simulates an unsafe
// consumer producing duplicate effects and verifies the expected invariant
// fails while others may still pass.
func TestUnsafeConsumerTriggersExpectedInvariantFailure(t *testing.T) {
	eng := NewEngine()
	for _, r := range DefaultRules() {
		eng.Register(r)
	}
	a100 := int64(100)
	ev := Evidence{
		RunID: "run-unsafe",
		Effects: []EffectRecord{
			effect("e1", "run-unsafe", "payment", "order-1", &a100, ptrString("pay-1"), baseTime()),
			// Unsafe mode replays the same idempotency key, creating a duplicate.
			effect("e2", "run-unsafe", "payment", "order-1", &a100, ptrString("pay-1"), baseTime().Add(time.Second)),
		},
	}
	results := eng.EvaluateAll(context.Background(), ev)

	// no_duplicate_effect must fail due to duplicate payment effect.
	dup := resultByKey(results, RuleNoDuplicateEffect)
	if dup.Status != StatusFail {
		t.Fatalf("expected no_duplicate_effect to fail for unsafe consumer, got %s", dup.Status)
	}

	// idempotency_key_unique must also fail due to repeated idempotency key.
	idem := resultByKey(results, RuleIdempotencyKeyUnique)
	if idem.Status != StatusFail {
		t.Fatalf("expected idempotency_key_unique to fail for unsafe consumer, got %s", idem.Status)
	}

	// At least one invariant must fail (the whole point of the unsafe fixture).
	anyFailed := false
	for _, res := range results {
		if res.Status == StatusFail {
			anyFailed = true
		}
	}
	if !anyFailed {
		t.Fatal("expected at least one invariant to fail for unsafe consumer")
	}
}
