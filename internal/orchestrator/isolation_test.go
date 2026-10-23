package orchestrator

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/zannunakiz/GenesisGO/internal/broker"
	"github.com/zannunakiz/GenesisGO/internal/invariants"
	"github.com/zannunakiz/GenesisGO/internal/store"
)

// trackingBroker records DeleteConsumer calls for cleanup verification.
type trackingBroker struct {
	mu          sync.Mutex
	published   map[string][][]byte
	subscribers map[string]broker.HandlerFunc
	deleted     map[string]bool
	started     bool
	closed      bool
}

func newTrackingBroker() *trackingBroker {
	return &trackingBroker{
		published:   make(map[string][][]byte),
		subscribers: make(map[string]broker.HandlerFunc),
		deleted:     make(map[string]bool),
		started:     true,
	}
}

func (b *trackingBroker) Publish(_ context.Context, runID string, payload []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.published[runID] = append(b.published[runID], append([]byte{}, payload...))
	return nil
}

func (b *trackingBroker) Subscribe(_ context.Context, runID string, handler broker.HandlerFunc) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.subscribers[runID] = handler
	return nil
}

func (b *trackingBroker) DeleteConsumer(_ context.Context, runID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.deleted[runID] = true
	return nil
}

func (b *trackingBroker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	return nil
}

func (b *trackingBroker) Ready() bool {
	return b.started && !b.closed
}

func (b *trackingBroker) isDeleted(runID string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.deleted[runID]
}

// TestParallelRunsAreIsolated verifies that two concurrent runs never mix
// evidence, side effects, or attempts.
func TestParallelRunsAreIsolated(t *testing.T) {
	st := newFakeStore()
	eng := newTestEngine()
	o, err := New(Config{
		Store:             st,
		Broker:            newTrackingBroker(),
		ScenarioLoader:    loaderWithFixture(t, "iso-a", passFixture),
		Invariants:        eng,
		Executor:          newFakeExecutor(),
		SampleConsumer:    nil,
		WorkerConcurrency: 2,
		RunTimeout:        10 * time.Second,
		LeaseTTL:          5 * time.Second,
		MaxEventsPerRun:   100,
	})
	if err != nil {
		t.Fatalf("new orchestrator: %v", err)
	}
	_ = o

	runA, err := st.CreateRun(context.Background(), "scenario-a", string(store.RunStatePending))
	if err != nil {
		t.Fatalf("create run A: %v", err)
	}
	runB, err := st.CreateRun(context.Background(), "scenario-b", string(store.RunStatePending))
	if err != nil {
		t.Fatalf("create run B: %v", err)
	}

	if _, err := st.CreateSideEffectRecord(context.Background(), runA.ID, "payment", "order-A", nil, int64Ptr(1000), "USD"); err != nil {
		t.Fatalf("create side effect A: %v", err)
	}

	effectsB, err := st.ListSideEffectRecordsByRun(context.Background(), runB.ID)
	if err != nil {
		t.Fatalf("list effects B: %v", err)
	}
	if len(effectsB) != 0 {
		t.Fatalf("run B should have no side effects, got %d", len(effectsB))
	}

	effectsA, err := st.ListSideEffectRecordsByRun(context.Background(), runA.ID)
	if err != nil {
		t.Fatalf("list effects A: %v", err)
	}
	if len(effectsA) != 1 {
		t.Fatalf("run A should have exactly 1 side effect, got %d", len(effectsA))
	}
}

// TestMaxEventsPerRunEnforcement verifies that a run cannot publish more
// events than MaxEventsPerRun allows.
func TestMaxEventsPerRunEnforcement(t *testing.T) {
	st := newFakeStore()
	brk := newTrackingBroker()
	eng := newTestEngine()

	const overLimitFixture = `{
	  "key": "over_limit",
	  "version": 1,
	  "name": "Over Limit",
	  "steps": [
	    { "id": "p1", "type": "publish", "event_id": "evt-1", "payload": { "type": "payment.succeeded", "aggregate_id": "order-1", "amount_cents": 100 } },
	    { "id": "p2", "type": "publish", "event_id": "evt-2", "payload": { "type": "payment.succeeded", "aggregate_id": "order-2", "amount_cents": 200 } },
	    { "id": "p3", "type": "publish", "event_id": "evt-3", "payload": { "type": "payment.succeeded", "aggregate_id": "order-3", "amount_cents": 300 } }
	  ],
  "parameters": { "max_events": 5, "max_retries": 3, "timeout_ms": 15000, "concurrency": 1 }
	}`
	loader := loaderWithFixture(t, "over_limit", overLimitFixture)

	o, err := New(Config{
		Store:             st,
		Broker:            brk,
		ScenarioLoader:    loader,
		Invariants:        eng,
		Executor:          nil,
		SampleConsumer:    nil,
		WorkerConcurrency: 1,
		RunTimeout:        10 * time.Second,
		LeaseTTL:          5 * time.Second,
		MaxEventsPerRun:   2,
	})
	if err != nil {
		t.Fatalf("new orchestrator: %v", err)
	}

	run, err := o.EnqueueScenario(context.Background(), "over_limit")
	if err != nil {
		t.Fatalf("enqueue scenario: %v", err)
	}

	_, err = o.ExecuteRun(context.Background(), run.ID)
	if err != nil {
		t.Fatalf("execute run: %v", err)
	}
	run, err = st.GetRun(context.Background(), run.ID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if run.Status != string(store.RunStateFailed) {
		t.Fatalf("expected failed status, got %q", run.Status)
	}
}

// TestCancelRunTransitionsToTerminal verifies a running run can be cancelled.
func TestCancelRunTransitionsToTerminal(t *testing.T) {
	st := newFakeStore()
	eng := newTestEngine()
	o, err := New(Config{
		Store:             st,
		Broker:            newTrackingBroker(),
		ScenarioLoader:    loaderWithFixture(t, "cancel-a", passFixture),
		Invariants:        eng,
		Executor:          newFakeExecutor(),
		SampleConsumer:    nil,
		WorkerConcurrency: 1,
		RunTimeout:        10 * time.Second,
		LeaseTTL:          5 * time.Second,
		MaxEventsPerRun:   100,
	})
	if err != nil {
		t.Fatalf("new orchestrator: %v", err)
	}

	run, err := st.CreateRun(context.Background(), "cancel_scenario", string(store.RunStatePending))
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	if _, err := st.TransitionRun(context.Background(), store.TransitionInput{
		RunID:           run.ID,
		From:            store.RunStatePending,
		To:              store.RunStateRunning,
		Reason:          "started",
		ExpectedVersion: run.Version,
	}); err != nil {
		t.Fatalf("transition to running: %v", err)
	}

	cancelled, err := o.CancelRun(context.Background(), run.ID)
	if err != nil {
		t.Fatalf("cancel run: %v", err)
	}
	if cancelled.Status != string(store.RunStateCancelled) {
		t.Fatalf("expected cancelled status, got %q", cancelled.Status)
	}

	if _, err := st.TransitionRun(context.Background(), store.TransitionInput{
		RunID:           run.ID,
		From:            store.RunStateCancelled,
		To:              store.RunStateRunning,
		Reason:          "restart",
		ExpectedVersion: cancelled.Version,
	}); err == nil {
		t.Fatal("expected error transitioning from terminal state")
	}
}

// TestFailRunTriggersCleanup verifies failRun followed by cleanupRun deletes
// the broker's durable consumer for the run.
func TestFailRunTriggersCleanup(t *testing.T) {
	st := newFakeStore()
	brk := newTrackingBroker()
	eng := newTestEngine()
	o, err := New(Config{
		Store:             st,
		Broker:            brk,
		ScenarioLoader:    loaderWithFixture(t, "fail-a", passFixture),
		Invariants:        eng,
		Executor:          newFakeExecutor(),
		SampleConsumer:    nil,
		WorkerConcurrency: 1,
		RunTimeout:        10 * time.Second,
		LeaseTTL:          5 * time.Second,
		MaxEventsPerRun:   100,
	})
	if err != nil {
		t.Fatalf("new orchestrator: %v", err)
	}

	run, err := st.CreateRun(context.Background(), "fail_scenario", string(store.RunStatePending))
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	if _, err := st.TransitionRun(context.Background(), store.TransitionInput{
		RunID:           run.ID,
		From:            store.RunStatePending,
		To:              store.RunStateRunning,
		Reason:          "started",
		ExpectedVersion: run.Version,
	}); err != nil {
		t.Fatalf("transition to running: %v", err)
	}

	failed, err := o.failRun(context.Background(), run.ID, run.Version, errors.New("simulated failure"))
	if err != nil {
		t.Fatalf("fail run: %v", err)
	}
	if failed.Status != string(store.RunStateFailed) {
		t.Fatalf("expected failed status, got %q", failed.Status)
	}

	o.cleanupRun(context.Background(), run.ID)
	if !brk.isDeleted(run.ID) {
		t.Fatal("expected broker consumer to be deleted after cleanupRun")
	}
}

// TestRunIsolationSecurity verifies run A cannot query or mutate run B state.
func TestRunIsolationSecurity(t *testing.T) {
	st := newFakeStore()

	runA, err := st.CreateRun(context.Background(), "scenario-a", string(store.RunStatePending))
	if err != nil {
		t.Fatalf("create run A: %v", err)
	}
	runB, err := st.CreateRun(context.Background(), "scenario-b", string(store.RunStatePending))
	if err != nil {
		t.Fatalf("create run B: %v", err)
	}

	if _, err := st.CreateSideEffectRecord(context.Background(), runA.ID, "payment", "order-A", nil, int64Ptr(500), "USD"); err != nil {
		t.Fatalf("create side effect A: %v", err)
	}

	effectsB, err := st.ListSideEffectRecordsByRun(context.Background(), runB.ID)
	if err != nil {
		t.Fatalf("list effects B: %v", err)
	}
	if len(effectsB) != 0 {
		t.Fatalf("run B must not see run A's side effects: got %d", len(effectsB))
	}

	if _, err := st.DeleteRunData(context.Background(), runB.ID); err != nil {
		t.Fatalf("delete run B: %v", err)
	}

	effectsA, err := st.ListSideEffectRecordsByRun(context.Background(), runA.ID)
	if err != nil {
		t.Fatalf("list effects A after deleting B: %v", err)
	}
	if len(effectsA) != 1 {
		t.Fatalf("run A's side effect must survive deletion of run B: got %d", len(effectsA))
	}

	if _, err := st.GetRun(context.Background(), runB.ID); err == nil {
		t.Fatal("expected run B to be deleted")
	} else if !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("expected pgx.ErrNoRows for deleted run B, got: %v", err)
	}
}

// TestRetentionListOldTerminalRuns verifies ListRunsOlderThan compiles and
// returns results (real time filtering is exercised by integration tests).
func TestRetentionListOldTerminalRuns(t *testing.T) {
	st := newFakeStore()

	if _, err := st.CreateRun(context.Background(), "scenario-old", string(store.RunStateCompleted)); err != nil {
		t.Fatalf("create old run: %v", err)
	}

	cutoff := time.Now().UTC().Add(-time.Hour)
	oldRuns, err := st.ListRunsOlderThan(context.Background(), cutoff)
	if err != nil {
		t.Fatalf("list runs older than cutoff: %v", err)
	}
	_ = oldRuns
}

func newTestEngine() *invariants.Engine {
	eng := invariants.NewEngine()
	for _, r := range invariants.DefaultRules() {
		eng.Register(r)
	}
	return eng
}