package orchestrator

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/zannunakiz/GenesisGO/internal/broker"
	"github.com/zannunakiz/GenesisGO/internal/invariants"
	"github.com/zannunakiz/GenesisGO/internal/sampleconsumer"
	"github.com/zannunakiz/GenesisGO/internal/scenarios"
	"github.com/zannunakiz/GenesisGO/internal/store"
)

const duplicateDeliveryFixture = `{
  "key": "duplicate_delivery",
  "version": 1,
  "name": "Duplicate Delivery",
  "description": "The same payment event is delivered twice to verify the consumer dedupes by idempotency key and does not double-charge.",
  "steps": [
    { "id": "publish-1", "type": "publish", "event_id": "evt-pay-001", "payload": { "type": "payment.succeeded", "aggregate_id": "order-100", "amount_cents": 1000, "currency": "USD", "idempotency_key": "pay-order-100" } },
    { "id": "deliver-1", "type": "deliver", "event_id": "evt-pay-001", "attempt": 1 },
    { "id": "ack-1", "type": "ack", "event_id": "evt-pay-001" },
    { "id": "deliver-2", "type": "deliver", "event_id": "evt-pay-001", "attempt": 2, "fault": "duplicate" },
    { "id": "ack-2", "type": "ack", "event_id": "evt-pay-001" }
  ],
  "parameters": { "max_events": 5, "max_retries": 3, "timeout_ms": 15000, "concurrency": 1 },
  "invariants": [
    { "key": "no_duplicate_payment", "type": "no_duplicate_effect", "effect_type": "payment", "aggregate": "order-100" },
    { "key": "idempotency_key_unique", "type": "idempotency_key_unique", "effect_type": "payment" }
  ],
  "seed": 42
}`

const outOfOrderFixture = `{
  "key": "out_of_order",
  "version": 1,
  "name": "Out-of-Order Delivery",
  "description": "Events are delivered in the order declared by the scenario plan, not the publish order, to verify the consumer applies inventory transitions correctly.",
  "steps": [
    { "id": "publish-1", "type": "publish", "event_id": "evt-inv-001", "payload": { "type": "inventory.adjusted", "aggregate_id": "sku-500", "amount_cents": -5, "currency": "USD", "idempotency_key": "inv-sku-500-decrement" } },
    { "id": "publish-2", "type": "publish", "event_id": "evt-inv-002", "payload": { "type": "inventory.adjusted", "aggregate_id": "sku-500", "amount_cents": 10, "currency": "USD", "idempotency_key": "inv-sku-500-increment" } },
    { "id": "deliver-2", "type": "deliver", "event_id": "evt-inv-002", "attempt": 1, "fault": "out_of_order" },
    { "id": "deliver-1", "type": "deliver", "event_id": "evt-inv-001", "attempt": 1 },
    { "id": "ack-1", "type": "ack", "event_id": "evt-inv-001" },
    { "id": "ack-2", "type": "ack", "event_id": "evt-inv-002" }
  ],
  "parameters": { "max_events": 5, "max_retries": 3, "timeout_ms": 20000, "concurrency": 1 },
  "invariants": [
    { "key": "allowed_order_transition", "type": "allowed_order_transition", "aggregate": "sku-500" },
    { "key": "no_negative_inventory", "type": "no_negative_inventory", "effect_type": "inventory", "aggregate": "sku-500" }
  ],
  "seed": 7
}`

// fakeBroker satisfies broker.Broker with an in-memory run-scoped publish log,
// keeping scenario execution deterministic without a live NATS server.
type fakeBroker struct {
	mu        sync.Mutex
	published map[string][][]byte
	subs      map[string]broker.HandlerFunc
	started   bool
	closed    bool
}

func newFakeBroker() *fakeBroker {
	return &fakeBroker{
		published: make(map[string][][]byte),
		subs:      make(map[string]broker.HandlerFunc),
	}
}

func (f *fakeBroker) Publish(_ context.Context, runID string, payload []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return broker.ErrAlreadyClosed
	}
	f.published[runID] = append(f.published[runID], append([]byte{}, payload...))
	return nil
}

func (f *fakeBroker) Subscribe(_ context.Context, runID string, handler broker.HandlerFunc) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return broker.ErrAlreadyClosed
	}
	f.subs[runID] = handler
	return nil
}

func (f *fakeBroker) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return broker.ErrAlreadyClosed
	}
	f.closed = true
	return nil
}

func (f *fakeBroker) Ready() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.started && !f.closed
}

func (f *fakeBroker) publishedCount(runID string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.published[runID])
}

// newScenarioExecutor builds an orchestrator wired to the in-memory fakes, the
// default (plan-driven) executor, and a sample consumer of the given mode.
func newScenarioExecutor(t *testing.T, mode sampleconsumer.Mode, fixture string) (*Orchestrator, *fakeStore, *fakeBroker) {
	t.Helper()
	st := newFakeStore()
	fb := newFakeBroker()
	dir := t.TempDir()
	if err := writeFixtureFile(dir, "scenario.json", fixture); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	loader := scenarios.NewLoader(dir)
	if _, err := loader.Load(); err != nil {
		t.Fatalf("load fixture: %v", err)
	}
	consumer, err := sampleconsumer.New(sampleconsumer.Config{Ledger: st, Mode: mode})
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	eng := invariants.NewEngine()
	for _, r := range invariants.DefaultRules() {
		eng.Register(r)
	}
	o, err := New(Config{
		Store:             st,
		Broker:            fb,
		ScenarioLoader:    loader,
		Invariants:        eng,
		SampleConsumer:    consumer,
		WorkerConcurrency: 1,
		RunTimeout:        10 * time.Second,
		LeaseTTL:          5 * time.Second,
	})
	if err != nil {
		t.Fatalf("new orchestrator: %v", err)
	}
	return o, st, fb
}

// invariantVerdict returns the recorded verdict for an invariant key.
func invariantVerdict(t *testing.T, st *fakeStore, runID, key string) string {
	t.Helper()
	results, err := st.ListInvariantResultsByRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("list invariant results: %v", err)
	}
	for _, r := range results {
		if r.InvariantKey == key {
			return r.Verdict
		}
	}
	return ""
}

// effectByKey returns the side effect with the given idempotency key.
func effectByKey(effects []*store.SideEffectRecord, key string) *store.SideEffectRecord {
	for _, e := range effects {
		if e.IdempotencyKey != nil && *e.IdempotencyKey == key {
			return e
		}
	}
	return nil
}

const negativeInventoryFixture = `{
  "key": "inventory_negative",
  "version": 1,
  "name": "Negative Inventory",
  "description": "A single inventory decrement on an empty aggregate must fail the no_negative_inventory invariant.",
  "steps": [
    { "id": "publish-1", "type": "publish", "event_id": "evt-inv-neg-1", "payload": { "type": "inventory.adjusted", "aggregate_id": "sku-900", "amount_cents": -5, "currency": "USD", "idempotency_key": "inv-sku-900-decrement" } },
    { "id": "deliver-1", "type": "deliver", "event_id": "evt-inv-neg-1", "attempt": 1 }
  ],
  "parameters": { "max_events": 5, "max_retries": 3, "timeout_ms": 15000, "concurrency": 1 },
  "invariants": [
    { "key": "no_negative_inventory", "type": "no_negative_inventory", "effect_type": "inventory", "aggregate": "sku-900" }
  ],
  "seed": 1
}`

const unknownEventFixture = `{
  "key": "unknown_event",
  "version": 1,
  "name": "Unknown Event",
  "description": "A deliver step referencing an event that was never published must fail the run.",
  "steps": [
    { "id": "deliver-1", "type": "deliver", "event_id": "evt-missing", "attempt": 1 }
  ],
  "parameters": { "max_events": 5, "max_retries": 3, "timeout_ms": 15000, "concurrency": 1 },
  "invariants": [
    { "key": "no_duplicate_effect", "type": "no_duplicate_effect", "effect_type": "payment", "aggregate": "order-9" }
  ],
  "seed": 1
}`

func TestDuplicateDeliverySafeConsumerMaintainsInvariant(t *testing.T) {
	o, st, fb := newScenarioExecutor(t, sampleconsumer.ModeSafe, duplicateDeliveryFixture)

	run, err := o.EnqueueScenario(context.Background(), "duplicate_delivery")
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := o.ExecuteRun(ctx, run.ID); err != nil {
		t.Fatalf("execute run: %v", err)
	}
	assertRunStatus(t, st, run.ID, string(store.RunStateCompleted))

	if got := fb.publishedCount(run.ID); got != 1 {
		t.Fatalf("expected 1 published event, got %d", got)
	}

	effects, err := st.ListSideEffectRecordsByRun(context.Background(), run.ID)
	if err != nil {
		t.Fatalf("list side effects: %v", err)
	}
	if len(effects) != 1 {
		t.Fatalf("safe consumer must dedupe duplicates: expected 1 payment effect, got %d", len(effects))
	}
	if effects[0].EffectType != "payment" || effects[0].AggregateID != "order-100" {
		t.Fatalf("unexpected effect: %+v", effects[0])
	}
	if effects[0].IdempotencyKey == nil || *effects[0].IdempotencyKey != "pay-order-100" {
		t.Fatalf("unexpected idempotency key: %+v", effects[0])
	}

	attempts := st.attemptsFor(run.ID, "evt-pay-001")
	if len(attempts) != 2 {
		t.Fatalf("expected exactly 2 delivery attempts, got %d", len(attempts))
	}
	if attempts[0].AttemptNumber != 1 || attempts[1].AttemptNumber != 2 {
		t.Fatalf("unexpected attempt numbers: %+v", attempts)
	}
	if attempts[1].AcknowledgedAt == nil {
		t.Fatal("expected the duplicate attempt to be acknowledged")
	}
	if attempts[1].FaultInjected == nil || *attempts[1].FaultInjected != "duplicate" {
		t.Fatalf("expected attempt 2 fault_injected=duplicate, got %v", attempts[1].FaultInjected)
	}

	if got := invariantVerdict(t, st, run.ID, "no_duplicate_payment"); got != "pass" {
		t.Fatalf("expected no_duplicate_payment to pass, got %q", got)
	}
	if got := invariantVerdict(t, st, run.ID, "idempotency_key_unique"); got != "pass" {
		t.Fatalf("expected idempotency_key_unique to pass, got %q", got)
	}
}

func TestDuplicateDeliveryUnsafeConsumerReproducesDuplicate(t *testing.T) {
	o, st, _ := newScenarioExecutor(t, sampleconsumer.ModeUnsafeDemo, duplicateDeliveryFixture)

	run, err := o.EnqueueScenario(context.Background(), "duplicate_delivery")
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := o.ExecuteRun(ctx, run.ID); err != nil {
		t.Fatalf("execute run: %v", err)
	}
	assertRunStatus(t, st, run.ID, string(store.RunStateFailed))

	effects, err := st.ListSideEffectRecordsByRun(context.Background(), run.ID)
	if err != nil {
		t.Fatalf("list side effects: %v", err)
	}
	if len(effects) != 2 {
		t.Fatalf("unsafe consumer must reproduce the duplicate effect: expected 2 payment effects, got %d", len(effects))
	}
	for _, e := range effects {
		if e.IdempotencyKey == nil || *e.IdempotencyKey != "pay-order-100" {
			t.Fatalf("unexpected idempotency key: %+v", e)
		}
	}

	if got := invariantVerdict(t, st, run.ID, "no_duplicate_payment"); got != "fail" {
		t.Fatalf("expected no_duplicate_payment to fail, got %q", got)
	}
	if got := invariantVerdict(t, st, run.ID, "idempotency_key_unique"); got != "fail" {
		t.Fatalf("expected idempotency_key_unique to fail, got %q", got)
	}

	attempts := st.attemptsFor(run.ID, "evt-pay-001")
	if len(attempts) != 2 || attempts[1].FaultInjected == nil || *attempts[1].FaultInjected != "duplicate" {
		t.Fatalf("expected 2 attempts with the duplicate fault on attempt 2, got %+v", attempts)
	}
}

func TestOutOfOrderDeliveryExecutesInPlannedOrder(t *testing.T) {
	o, st, fb := newScenarioExecutor(t, sampleconsumer.ModeSafe, outOfOrderFixture)

	run, err := o.EnqueueScenario(context.Background(), "out_of_order")
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := o.ExecuteRun(ctx, run.ID); err != nil {
		t.Fatalf("execute run: %v", err)
	}
	assertRunStatus(t, st, run.ID, string(store.RunStateCompleted))

	if got := fb.publishedCount(run.ID); got != 2 {
		t.Fatalf("expected 2 published events, got %d", got)
	}

	effects, err := st.ListSideEffectRecordsByRun(context.Background(), run.ID)
	if err != nil {
		t.Fatalf("list side effects: %v", err)
	}
	if len(effects) != 2 {
		t.Fatalf("expected 2 inventory effects, got %d", len(effects))
	}
	first := effectByKey(effects, "inv-sku-500-increment")
	second := effectByKey(effects, "inv-sku-500-decrement")
	if first == nil || second == nil {
		t.Fatalf("expected both inventory effects, got %+v", effects)
	}
	effectsByIndex := map[*store.SideEffectRecord]int{}
	for i, e := range effects {
		effectsByIndex[e] = i
	}
	if effectsByIndex[first] >= effectsByIndex[second] {
		t.Fatalf("expected the increment to be delivered before the decrement, got %+v", effects)
	}
	if *first.AmountCents != 10 || *second.AmountCents != -5 {
		t.Fatalf("unexpected effect amounts: %+v %+v", first, second)
	}

	attempts2 := st.attemptsFor(run.ID, "evt-inv-002")
	if len(attempts2) != 1 || attempts2[0].AttemptNumber != 1 {
		t.Fatalf("expected one attempt for evt-inv-002, got %+v", attempts2)
	}
	if attempts2[0].FaultInjected == nil || *attempts2[0].FaultInjected != "out_of_order" {
		t.Fatalf("expected the out_of_order fault on evt-inv-002, got %v", attempts2[0].FaultInjected)
	}
	attempts1 := st.attemptsFor(run.ID, "evt-inv-001")
	if len(attempts1) != 1 || attempts1[0].FaultInjected != nil {
		t.Fatalf("expected no injected fault on evt-inv-001, got %+v", attempts1)
	}

	if got := invariantVerdict(t, st, run.ID, "no_negative_inventory"); got != "pass" {
		t.Fatalf("expected no_negative_inventory to pass (final balance 5), got %q", got)
	}
	if got := invariantVerdict(t, st, run.ID, "allowed_order_transition"); got != "pass" {
		t.Fatalf("expected allowed_order_transition to pass, got %q", got)
	}
}

func TestOutOfOrderNegativeInventoryFails(t *testing.T) {
	o, st, _ := newScenarioExecutor(t, sampleconsumer.ModeSafe, negativeInventoryFixture)

	run, err := o.EnqueueScenario(context.Background(), "inventory_negative")
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := o.ExecuteRun(ctx, run.ID); err != nil {
		t.Fatalf("execute run: %v", err)
	}
	assertRunStatus(t, st, run.ID, string(store.RunStateFailed))

	effects, err := st.ListSideEffectRecordsByRun(context.Background(), run.ID)
	if err != nil {
		t.Fatalf("list side effects: %v", err)
	}
	if len(effects) != 1 || *effects[0].AmountCents != -5 {
		t.Fatalf("expected one -5 inventory effect, got %+v", effects)
	}
	if got := invariantVerdict(t, st, run.ID, "no_negative_inventory"); got != "fail" {
		t.Fatalf("expected no_negative_inventory to fail, got %q", got)
	}
}

func TestDeliverUnknownEventFailsRun(t *testing.T) {
	o, st, _ := newScenarioExecutor(t, sampleconsumer.ModeSafe, unknownEventFixture)

	run, err := o.EnqueueScenario(context.Background(), "unknown_event")
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := o.ExecuteRun(ctx, run.ID); err != nil {
		t.Fatalf("execute run: %v", err)
	}
	assertRunStatus(t, st, run.ID, string(store.RunStateFailed))
}

const retryAfterTimeoutFixture = `{
  "key": "retry_after_timeout",
  "version": 1,
  "name": "Retry After Timeout",
  "steps": [
    { "id": "publish-1", "type": "publish", "event_id": "evt-pay-002", "payload": { "type": "payment.succeeded", "aggregate_id": "order-200", "amount_cents": 2500, "currency": "USD", "idempotency_key": "pay-order-200" } },
    { "id": "deliver-1", "type": "deliver", "event_id": "evt-pay-002", "attempt": 1, "fault": "retry" },
    { "id": "delay-1", "type": "delay", "delay_ms": 500 },
    { "id": "deliver-2", "type": "deliver", "event_id": "evt-pay-002", "attempt": 2 },
    { "id": "ack-2", "type": "ack", "event_id": "evt-pay-002" }
  ],
  "parameters": { "max_events": 5, "max_retries": 3, "timeout_ms": 25000, "concurrency": 1 },
  "invariants": [
    { "key": "no_duplicate_payment", "type": "no_duplicate_effect", "effect_type": "payment", "aggregate": "order-200" },
    { "key": "idempotency_key_unique", "type": "idempotency_key_unique", "effect_type": "payment" }
  ],
  "seed": 99
}`

const crashAfterCommitBeforeAckFixture = `{
  "key": "crash_after_commit_before_ack",
  "version": 1,
  "name": "Crash After Commit Before Ack",
  "steps": [
    { "id": "publish-1", "type": "publish", "event_id": "evt-pay-004", "payload": { "type": "payment.succeeded", "aggregate_id": "order-400", "amount_cents": 750, "currency": "USD", "idempotency_key": "pay-order-400" } },
    { "id": "deliver-1", "type": "deliver", "event_id": "evt-pay-004", "attempt": 1 },
    { "id": "crash-1", "type": "crash", "fault": "crash_after_commit" },
    { "id": "deliver-2", "type": "deliver", "event_id": "evt-pay-004", "attempt": 2 },
    { "id": "ack-2", "type": "ack", "event_id": "evt-pay-004" }
  ],
  "parameters": { "max_events": 5, "max_retries": 3, "timeout_ms": 20000, "concurrency": 1 },
  "invariants": [
    { "key": "no_duplicate_payment", "type": "no_duplicate_effect", "effect_type": "payment", "aggregate": "order-400" },
    { "key": "idempotency_key_unique", "type": "idempotency_key_unique", "effect_type": "payment" }
  ],
  "seed": 1337
}`

const workerRestartFixture = `{
  "key": "worker_restart",
  "version": 1,
  "name": "Worker Restart",
  "steps": [
    { "id": "publish-1", "type": "publish", "event_id": "evt-pay-003", "payload": { "type": "payment.succeeded", "aggregate_id": "order-300", "amount_cents": 500, "currency": "USD", "idempotency_key": "pay-order-300" } },
    { "id": "deliver-1", "type": "deliver", "event_id": "evt-pay-003", "attempt": 1 },
    { "id": "restart-1", "type": "restart" },
    { "id": "deliver-2", "type": "deliver", "event_id": "evt-pay-003", "attempt": 2 },
    { "id": "ack-2", "type": "ack", "event_id": "evt-pay-003" }
  ],
  "parameters": { "max_events": 5, "max_retries": 3, "timeout_ms": 20000, "concurrency": 1 },
  "invariants": [
    { "key": "no_duplicate_payment", "type": "no_duplicate_effect", "effect_type": "payment", "aggregate": "order-300" },
    { "key": "idempotency_key_unique", "type": "idempotency_key_unique", "effect_type": "payment" }
  ],
  "seed": 2024
}`

// attemptFaults returns the fault_injected value for a run's attempts of one
// event, in record order (empty string when no fault was injected).
func attemptFaults(t *testing.T, st *fakeStore, runID, eventID string) []string {
	t.Helper()
	var out []string
	for _, a := range st.attemptsFor(runID, eventID) {
		if a.FaultInjected != nil {
			out = append(out, *a.FaultInjected)
		} else {
			out = append(out, "")
		}
	}
	return out
}

func TestRetryAfterTimeoutAppliesEffectOnce(t *testing.T) {
	o, st, fb := newScenarioExecutor(t, sampleconsumer.ModeSafe, retryAfterTimeoutFixture)

	run, err := o.EnqueueScenario(context.Background(), "retry_after_timeout")
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := o.ExecuteRun(ctx, run.ID); err != nil {
		t.Fatalf("execute run: %v", err)
	}
	assertRunStatus(t, st, run.ID, string(store.RunStateCompleted))

	if got := fb.publishedCount(run.ID); got != 1 {
		t.Fatalf("expected 1 published event, got %d", got)
	}

	effects, err := st.ListSideEffectRecordsByRun(context.Background(), run.ID)
	if err != nil {
		t.Fatalf("list side effects: %v", err)
	}
	if len(effects) != 1 {
		t.Fatalf("expected exactly 1 payment effect, got %d", len(effects))
	}
	if effects[0].AggregateID != "order-200" || *effects[0].AmountCents != 2500 {
		t.Fatalf("unexpected effect: %+v", effects[0])
	}

	attempts := st.attemptsFor(run.ID, "evt-pay-002")
	if len(attempts) != 2 {
		t.Fatalf("expected exactly 2 delivery attempts, got %d", len(attempts))
	}
	if attempts[0].AttemptNumber != 1 || attempts[0].AcknowledgedAt != nil {
		t.Fatalf("attempt 1 should be unacked retryable failure, got %+v", attempts[0])
	}
	if attempts[0].FaultInjected == nil || *attempts[0].FaultInjected != "retry" {
		t.Fatalf("expected attempt 1 fault_injected=retry, got %v", attempts[0].FaultInjected)
	}
	if attempts[1].AttemptNumber != 2 || attempts[1].AcknowledgedAt == nil {
		t.Fatalf("attempt 2 should be acked redelivery, got %+v", attempts[1])
	}

	if got := invariantVerdict(t, st, run.ID, "no_duplicate_payment"); got != "pass" {
		t.Fatalf("expected no_duplicate_payment to pass, got %q", got)
	}
	if got := invariantVerdict(t, st, run.ID, "idempotency_key_unique"); got != "pass" {
		t.Fatalf("expected idempotency_key_unique to pass, got %q", got)
	}
}

func TestCrashAfterCommitBeforeAckRedeliversCommittedEvent(t *testing.T) {
	o, st, _ := newScenarioExecutor(t, sampleconsumer.ModeSafe, crashAfterCommitBeforeAckFixture)

	run, err := o.EnqueueScenario(context.Background(), "crash_after_commit_before_ack")
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := o.ExecuteRun(ctx, run.ID); err != nil {
		t.Fatalf("execute run: %v", err)
	}
	assertRunStatus(t, st, run.ID, string(store.RunStateCompleted))

	effects, err := st.ListSideEffectRecordsByRun(context.Background(), run.ID)
	if err != nil {
		t.Fatalf("list side effects: %v", err)
	}
	if len(effects) != 1 {
		t.Fatalf("expected exactly 1 payment effect, got %d", len(effects))
	}
	if effects[0].AggregateID != "order-400" {
		t.Fatalf("unexpected effect: %+v", effects[0])
	}

	attempts := st.attemptsFor(run.ID, "evt-pay-004")
	if len(attempts) != 2 {
		t.Fatalf("expected exactly 2 delivery attempts, got %d", len(attempts))
	}
	if attempts[0].AttemptNumber != 1 || attempts[0].AcknowledgedAt == nil {
		t.Fatalf("attempt 1 should be committed and acked, got %+v", attempts[0])
	}
	if attempts[1].AttemptNumber != 2 || attempts[1].AcknowledgedAt != nil {
		t.Fatalf("attempt 2 crashed before ack, got %+v", attempts[1])
	}
	if attempts[1].FaultInjected == nil || *attempts[1].FaultInjected != "crash_after_commit" {
		t.Fatalf("expected attempt 2 fault_injected=crash_after_commit, got %v", attempts[1].FaultInjected)
	}

	if got := invariantVerdict(t, st, run.ID, "no_duplicate_payment"); got != "pass" {
		t.Fatalf("expected no_duplicate_payment to pass, got %q", got)
	}
	if got := invariantVerdict(t, st, run.ID, "idempotency_key_unique"); got != "pass" {
		t.Fatalf("expected idempotency_key_unique to pass, got %q", got)
	}
}

func TestWorkerRestartRecoversWithoutDuplicate(t *testing.T) {
	o, st, _ := newScenarioExecutor(t, sampleconsumer.ModeSafe, workerRestartFixture)

	run, err := o.EnqueueScenario(context.Background(), "worker_restart")
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := o.ExecuteRun(ctx, run.ID); err != nil {
		t.Fatalf("execute run: %v", err)
	}
	assertRunStatus(t, st, run.ID, string(store.RunStateCompleted))

	effects, err := st.ListSideEffectRecordsByRun(context.Background(), run.ID)
	if err != nil {
		t.Fatalf("list side effects: %v", err)
	}
	if len(effects) != 1 {
		t.Fatalf("expected exactly 1 payment effect, got %d", len(effects))
	}
	if effects[0].AggregateID != "order-300" || *effects[0].AmountCents != 500 {
		t.Fatalf("unexpected effect: %+v", effects[0])
	}

	attempts := st.attemptsFor(run.ID, "evt-pay-003")
	if len(attempts) != 2 {
		t.Fatalf("expected exactly 2 delivery attempts, got %d", len(attempts))
	}
	for _, a := range attempts {
		if a.AcknowledgedAt == nil {
			t.Fatalf("expected both attempts acknowledged after restart recovery, got %+v", a)
		}
		if a.FaultInjected != nil {
			t.Fatalf("expected no injected fault after restart, got %v", a.FaultInjected)
		}
	}

	if got := invariantVerdict(t, st, run.ID, "no_duplicate_payment"); got != "pass" {
		t.Fatalf("expected no_duplicate_payment to pass, got %q", got)
	}
	if got := invariantVerdict(t, st, run.ID, "idempotency_key_unique"); got != "pass" {
		t.Fatalf("expected idempotency_key_unique to pass, got %q", got)
	}
}
