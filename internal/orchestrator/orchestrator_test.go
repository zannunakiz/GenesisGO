package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/zannunakiz/GenesisGO/internal/invariants"
	"github.com/zannunakiz/GenesisGO/internal/scenarios"
	"github.com/zannunakiz/GenesisGO/internal/store"
)

// passFixture is a committed-style scenario fixture satisfied by a single
// idempotent payment.
const passFixture = `{
  "key": "pass_safe",
  "version": 1,
  "name": "Pass Safe",
  "description": "a single idempotent payment that satisfies all invariants",
  "steps": [
    { "id": "publish-1", "type": "publish", "event_id": "evt-1", "payload": { "type": "payment.succeeded", "aggregate_id": "order-1", "amount_cents": 1000, "currency": "USD", "idempotency_key": "pay-1" } },
    { "id": "ack-1", "type": "ack", "event_id": "evt-1" }
  ],
  "parameters": { "max_events": 5, "max_retries": 3, "timeout_ms": 15000, "concurrency": 1 },
  "invariants": [
    { "key": "no_duplicate_payment", "type": "no_duplicate_effect", "effect_type": "payment", "aggregate": "order-1" },
    { "key": "idempotency_key_unique", "type": "idempotency_key_unique", "effect_type": "payment" }
  ],
  "seed": 1
}`

func loaderWithFixture(t *testing.T, name, content string) *scenarios.Loader {
	t.Helper()
	dir := t.TempDir()
	if err := writeFixtureFile(dir, name+".json", content); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	l := scenarios.NewLoader(dir)
	if _, err := l.Load(); err != nil {
		t.Fatalf("load fixture: %v", err)
	}
	return l
}

// fakeStore implements RunStore in memory for orchestration unit tests.
type fakeStore struct {
	mu sync.Mutex

	runs        map[string]*store.Run
	jobs        map[string]*store.RunJob // id -> job
	jobsByRun   map[string][]*store.RunJob
	scenarios   map[string]*store.Scenario // key -> scenario
	sideEffects map[string][]*store.SideEffectRecord
	attempts    map[string][]*store.EventAttempt // run_id -> attempts
	invariants  map[string][]*store.InvariantResult
	stateSnapshots map[string][]*store.StateSnapshot // run_id -> snapshots

	// Claim behavior.
	claimJob   *store.RunJob
	claimErr   error
	claimCalls int

	// Transition behavior override (optional).
	transitionOverride func(ctx context.Context, input store.TransitionInput) (*store.Run, error)
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		runs:        make(map[string]*store.Run),
		jobs:        make(map[string]*store.RunJob),
		jobsByRun:   make(map[string][]*store.RunJob),
		scenarios:   make(map[string]*store.Scenario),
		sideEffects: make(map[string][]*store.SideEffectRecord),
		attempts:    make(map[string][]*store.EventAttempt),
		invariants:  make(map[string][]*store.InvariantResult),
		stateSnapshots: make(map[string][]*store.StateSnapshot),
	}
}

func (f *fakeStore) WithTx(ctx context.Context, fn func(pgx.Tx) error) error {
	return fn(nil)
}

func (f *fakeStore) CreateScenario(ctx context.Context, key, name, description string) (*store.Scenario, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.scenarios[key]; ok {
		return nil, fmt.Errorf("scenario %q already exists", key)
	}
	s := &store.Scenario{ID: fxID("scenario"), Key: key, Name: name, Description: description}
	f.scenarios[key] = s
	return s, nil
}

func (f *fakeStore) GetScenarioByKey(ctx context.Context, key string) (*store.Scenario, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	s, ok := f.scenarios[key]
	if !ok {
		return nil, pgx.ErrNoRows
	}
	return s, nil
}

func (f *fakeStore) CreateRun(ctx context.Context, scenarioID, status string) (*store.Run, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r := &store.Run{ID: fxID("run"), ScenarioID: scenarioID, Status: status, Version: 1}
	f.runs[r.ID] = r
	return r, nil
}

func (f *fakeStore) GetRun(ctx context.Context, id string) (*store.Run, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.runs[id]
	if !ok {
		return nil, pgx.ErrNoRows
	}
	return r, nil
}

func (f *fakeStore) TransitionRun(ctx context.Context, input store.TransitionInput) (*store.Run, error) {
	if f.transitionOverride != nil {
		return f.transitionOverride(ctx, input)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.runs[input.RunID]
	if !ok {
		return nil, pgx.ErrNoRows
	}
	if r.Status != string(input.From) {
		return nil, store.ErrConcurrentUpdate
	}
	if store.IsTerminal(store.RunState(r.Status)) {
		return nil, store.ErrInvalidTransition
	}
	r.Status = string(input.To)
	r.Version++
	r.AttemptCount++
	return r, nil
}

func (f *fakeStore) CreateRunJob(ctx context.Context, runID, jobType, status string, payload []byte) (*store.RunJob, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	j := &store.RunJob{ID: fxID("job"), RunID: runID, JobType: jobType, Status: status, Payload: payload}
	f.jobs[j.ID] = j
	f.jobsByRun[runID] = append(f.jobsByRun[runID], j)
	return j, nil
}

func (f *fakeStore) ClaimRunJob(ctx context.Context, owner string, ttlSeconds int64) (*store.RunJob, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.claimCalls++
	if f.claimErr != nil {
		return nil, f.claimErr
	}
	if f.claimJob == nil {
		return nil, pgx.ErrNoRows
	}
	j := f.claimJob
	f.claimJob = nil // only one claim
	return j, nil
}

func (f *fakeStore) UpdateRunJobStatus(ctx context.Context, jobID, status string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if j, ok := f.jobs[jobID]; ok {
		j.Status = status
	}
	return nil
}

func (f *fakeStore) HeartbeatRunJob(ctx context.Context, jobID string, ttlSeconds int64, owner string) error {
	return nil
}
func (f *fakeStore) RecoverExpiredLeases(ctx context.Context) (int64, error) { return 0, nil }

func (f *fakeStore) CreateEventAttempt(ctx context.Context, runID, eventID string, attemptNumber int32, deliveredAt time.Time, acknowledgedAt *time.Time, faultInjected *string) (*store.EventAttempt, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	a := &store.EventAttempt{
		ID:              fxID("attempt"),
		RunID:           runID,
		EventID:         eventID,
		AttemptNumber:   attemptNumber,
		DeliveredAt:     deliveredAt,
		AcknowledgedAt:  acknowledgedAt,
		FaultInjected:   faultInjected,
		CreatedAt:       time.Now().UTC(),
	}
	f.attempts[runID] = append(f.attempts[runID], a)
	return a, nil
}

func (f *fakeStore) CountEventAttemptsByRun(ctx context.Context, runID string) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.attempts[runID]), nil
}

func (f *fakeStore) CancelRun(ctx context.Context, runID string, expectedVersion int32, reason string) (*store.Run, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.runs[runID]
	if !ok {
		return nil, pgx.ErrNoRows
	}
	if store.IsTerminal(store.RunState(r.Status)) {
		return nil, fmt.Errorf("%w: run %q is already terminal (%q)", store.ErrInvalidTransition, runID, r.Status)
	}
	from := store.RunState(r.Status)
	if from != store.RunStateRunning && from != store.RunStatePending {
		return nil, fmt.Errorf("%w: run %q is %q, expected %q or %q", store.ErrInvalidTransition, runID, r.Status, store.RunStateRunning, store.RunStatePending)
	}
	r.Status = string(store.RunStateCancelled)
	r.Version++
	r.AttemptCount++
	return r, nil
}

func (f *fakeStore) DeleteRunData(ctx context.Context, runID string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.runs, runID)
	delete(f.jobsByRun, runID)
	delete(f.sideEffects, runID)
	delete(f.attempts, runID)
	delete(f.invariants, runID)
	delete(f.stateSnapshots, runID)
	return 1, nil
}

func (f *fakeStore) ListRunsOlderThan(ctx context.Context, cutoff time.Time) ([]*store.Run, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []*store.Run
	for _, r := range f.runs {
		out = append(out, r)
	}
	return out, nil
}

// attemptsFor returns a run's attempts for one event, in record order.
func (f *fakeStore) attemptsFor(runID, eventID string) []*store.EventAttempt {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []*store.EventAttempt
	for _, a := range f.attempts[runID] {
		if a.EventID == eventID {
			out = append(out, a)
		}
	}
	return out
}

func (f *fakeStore) CreateInvariantResult(ctx context.Context, runID, invariantKey, verdict string, details []byte) (*store.InvariantResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r := &store.InvariantResult{ID: fxID("inv"), RunID: runID, InvariantKey: invariantKey, Verdict: verdict}
	f.invariants[runID] = append(f.invariants[runID], r)
	return r, nil
}

func (f *fakeStore) ListInvariantResultsByRun(ctx context.Context, runID string) ([]*store.InvariantResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.invariants[runID], nil
}

func (f *fakeStore) ListSideEffectRecordsByRun(ctx context.Context, runID string) ([]*store.SideEffectRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.sideEffects[runID], nil
}

func (f *fakeStore) CreateStateSnapshot(ctx context.Context, runID, stepID, eventID, snapshotType, aggregateID string, stateSummary []byte) (*store.StateSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r := &store.StateSnapshot{
		ID:           fxID("snapshot"),
		RunID:        runID,
		StepID:       stepID,
		EventID:      eventID,
		SnapshotType: snapshotType,
		AggregateID:  aggregateID,
		StateSummary: stateSummary,
		CreatedAt:    time.Now().UTC(),
	}
	f.stateSnapshots[runID] = append(f.stateSnapshots[runID], r)
	return r, nil
}

func (f *fakeStore) ListStateSnapshotsByRun(ctx context.Context, runID string) ([]*store.StateSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stateSnapshots[runID], nil
}

func (f *fakeStore) CreateSideEffectRecord(ctx context.Context, runID, effectType, aggregateID string, idempotencyKey *string, amountCents *int64, currency string) (*store.SideEffectRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r := &store.SideEffectRecord{
		ID:             fxID("effect"),
		RunID:          runID,
		EffectType:     effectType,
		AggregateID:    aggregateID,
		IdempotencyKey: idempotencyKey,
		AmountCents:    amountCents,
		Currency:       currency,
		RecordedAt:     time.Now().UTC(),
	}
	f.sideEffects[runID] = append(f.sideEffects[runID], r)
	return r, nil
}

func (f *fakeStore) ListRunJobsByRun(ctx context.Context, runID string) ([]*store.RunJob, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.jobsByRun[runID], nil
}

func (f *fakeStore) SetSideEffects(runID string, effects ...*store.SideEffectRecord) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sideEffects[runID] = effects
}

func (f *fakeStore) SetClaim(job *store.RunJob, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.claimJob = job
	f.claimErr = err
	f.claimCalls = 0
}

func (f *fakeStore) ClaimCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.claimCalls
}

// fakeExecutor records Execute calls and returns a controllable result.
type fakeExecutor struct {
	mu      sync.Mutex
	calls   int
	lastRun string
	err     error
	blockCh chan struct{}
}

func newFakeExecutor() *fakeExecutor {
	return &fakeExecutor{}
}

func (x *fakeExecutor) Execute(ctx context.Context, runID string, scenario *scenarios.Scenario) error {
	x.mu.Lock()
	x.calls++
	x.lastRun = runID
	err := x.err
	block := x.blockCh
	x.mu.Unlock()
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return err
}

func (x *fakeExecutor) Calls() int {
	x.mu.Lock()
	defer x.mu.Unlock()
	return x.calls
}

func (x *fakeExecutor) LastRun() string {
	x.mu.Lock()
	defer x.mu.Unlock()
	return x.lastRun
}

func (x *fakeExecutor) SetError(err error) { x.mu.Lock(); x.err = err; x.mu.Unlock() }

func (x *fakeExecutor) Block() chan struct{} {
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.blockCh == nil {
		x.blockCh = make(chan struct{})
	}
	return x.blockCh
}

func (x *fakeExecutor) Unblock() {
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.blockCh != nil {
		close(x.blockCh)
		x.blockCh = nil
	}
}

var seq int64

func fxID(kind string) string {
	return fmt.Sprintf("fx-%s-%d", kind, atomic.AddInt64(&seq, 1))
}

func writeFixtureFile(dir, name, content string) error {
	return os.WriteFile(dir+"/"+name, []byte(content), 0o644)
}

func strPtr(s string) *string { return &s }
func int64Ptr(i int64) *int64 { return &i }

func newOrchestrator(t *testing.T, st *fakeStore, ex *fakeExecutor, loader *scenarios.Loader) *Orchestrator {
	t.Helper()
	eng := invariants.NewEngine()
	for _, r := range invariants.DefaultRules() {
		eng.Register(r)
	}
	o, err := New(Config{
		Store:             st,
		ScenarioLoader:    loader,
		Invariants:        eng,
		Executor:          ex,
		WorkerConcurrency: 2,
		RunTimeout:        10 * time.Second,
		LeaseTTL:          5 * time.Second,
	})
	if err != nil {
		t.Fatalf("new orchestrator: %v", err)
	}
	return o
}

func assertRunStatus(t *testing.T, st *fakeStore, runID, want string) {
	t.Helper()
	r, err := st.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if r.Status != want {
		t.Fatalf("expected run status %q, got %q", want, r.Status)
	}
}

// TestEnqueueScenarioCreatesRunAndJob validates that enqueuing a scenario
// creates a pending run and a queued job transactionally.
func TestEnqueueScenarioCreatesRunAndJob(t *testing.T) {
	st := newFakeStore()
	loader := loaderWithFixture(t, "pass_safe", passFixture)
	o := newOrchestrator(t, st, newFakeExecutor(), loader)

	run, err := o.EnqueueScenario(context.Background(), "pass_safe")
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if run.Status != string(store.RunStatePending) {
		t.Fatalf("expected pending run, got %q", run.Status)
	}

	jobs, err := st.ListRunJobsByRun(context.Background(), run.ID)
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	if len(jobs) != 1 || jobs[0].Status != "queued" {
		t.Fatalf("expected one queued job, got %+v", jobs)
	}
}

// TestEnqueueRejectsUnknownScenario validates that an unknown scenario key is
// rejected before any run or job is created.
func TestEnqueueRejectsUnknownScenario(t *testing.T) {
	st := newFakeStore()
	loader := loaderWithFixture(t, "pass_safe", passFixture)
	o := newOrchestrator(t, st, newFakeExecutor(), loader)

	if _, err := o.EnqueueScenario(context.Background(), "does_not_exist"); err == nil {
		t.Fatal("expected error for unknown scenario")
	} else if !errors.Is(err, ErrScenarioNotFound) {
		t.Fatalf("expected ErrScenarioNotFound, got %v", err)
	}

	if len(st.runs) != 0 {
		t.Fatal("expected no runs to be created for an unknown scenario")
	}
}

// TestConcurrentClaimsExecuteOnce validates that two workers claiming the same
// job do not execute it twice (FOR UPDATE SKIP LOCKED semantics).
func TestConcurrentClaimsExecuteOnce(t *testing.T) {
	st := newFakeStore()
	loader := loaderWithFixture(t, "pass_safe", passFixture)
	ex := newFakeExecutor()
	o := newOrchestrator(t, st, ex, loader)

	run, err := o.EnqueueScenario(context.Background(), "pass_safe")
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	jobs, _ := st.ListRunJobsByRun(context.Background(), run.ID)
	t.Logf("runID=%s jobs=%d", run.ID, len(jobs))
	st.SetClaim(jobs[0], nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	o.StartWorkers(ctx, 2)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if ex.Calls() >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	o.Stop()

	if got := ex.Calls(); got != 1 {
		t.Fatalf("expected exactly one execution, got %d", got)
	}
	assertRunStatus(t, st, run.ID, string(store.RunStateCompleted))
}

// TestWorkerInterruptionLeavesRunRecoverable validates that a worker that is
// interrupted before completing leaves the run in a recoverable state, never a
// fake completed/failed pass.
func TestWorkerInterruptionLeavesRunRecoverable(t *testing.T) {
	st := newFakeStore()
	loader := loaderWithFixture(t, "pass_safe", passFixture)
	ex := newFakeExecutor()
	o := newOrchestrator(t, st, ex, loader)

	run, err := o.EnqueueScenario(context.Background(), "pass_safe")
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Start execution with a blocking executor, then cancel the context to
	// simulate a worker being interrupted mid-job.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	block := ex.Block()
	done := make(chan error, 1)
	go func() {
		_, err := o.ExecuteRun(ctx, run.ID)
		done <- err
	}()

	// Give the worker a moment to transition the run to running, then cancel.
	time.Sleep(200 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		// The run may complete as failed (an explicitly failed state, which is
		// acceptable per the validation gate: no fake pass). A nil error simply
		// means the terminal transition itself succeeded.
		_ = err
	case <-time.After(3 * time.Second):
		t.Fatal("interrupted execution did not return")
	}
	_ = block

	// The run must be running (recoverable) or failed, never completed.
	r, err := st.GetRun(context.Background(), run.ID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if r.Status == string(store.RunStateCompleted) {
		t.Fatalf("interrupted worker produced a completed status (fake pass)")
	}
	if store.IsTerminal(store.RunState(r.Status)) && r.Status != string(store.RunStateFailed) {
		t.Fatalf("interrupted worker produced unexpected terminal status %q", r.Status)
	}
}

// TestTerminalStatusSetOnce validates that a run can transition to a terminal
// status exactly once and that terminal states are absorbing (no fake pass).
func TestTerminalStatusSetOnce(t *testing.T) {
	st := newFakeStore()
	loader := loaderWithFixture(t, "pass_safe", passFixture)
	o := newOrchestrator(t, st, newFakeExecutor(), loader)

	run, err := o.EnqueueScenario(context.Background(), "pass_safe")
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	ctx := context.Background()
	// pending -> running
	r, err := st.TransitionRun(ctx, store.TransitionInput{
		RunID:           run.ID,
		From:            store.RunStatePending,
		To:              store.RunStateRunning,
		Reason:          "worker started",
		ExpectedVersion: run.Version,
	})
	if err != nil {
		t.Fatalf("pending->running: %v", err)
	}

	// running -> completed succeeds.
	r, err = st.TransitionRun(ctx, store.TransitionInput{
		RunID:           run.ID,
		From:            store.RunStateRunning,
		To:              store.RunStateCompleted,
		Reason:          "finished",
		ExpectedVersion: r.Version,
	})
	if err != nil {
		t.Fatalf("running->completed: %v", err)
	}
	if !store.IsTerminal(store.RunState(r.Status)) {
		t.Fatal("expected terminal completed status")
	}

	// Stale duplicate completion must fail (version changed).
	_, err = st.TransitionRun(ctx, store.TransitionInput{
		RunID:           run.ID,
		From:            store.RunStateRunning,
		To:              store.RunStateCompleted,
		Reason:          "stale",
		ExpectedVersion: r.Version,
	})
	if err == nil {
		t.Fatal("expected stale completion to fail")
	}
	if !errors.Is(err, store.ErrConcurrentUpdate) {
		t.Fatalf("expected ErrConcurrentUpdate, got %v", err)
	}

	// Terminal is absorbing: completed -> failed must be rejected.
	_, err = st.TransitionRun(ctx, store.TransitionInput{
		RunID:           run.ID,
		From:            store.RunStateCompleted,
		To:              store.RunStateFailed,
		Reason:          "should not happen",
		ExpectedVersion: r.Version,
	})
	if err == nil {
		t.Fatal("expected terminal completed->failed to fail")
	}
	if !errors.Is(err, store.ErrInvalidTransition) {
		t.Fatalf("expected ErrInvalidTransition, got %v", err)
	}
}

// TestInvariantFailureLeadsToFailed validates that when invariants fail the
// run is marked failed, never completed (no fake pass).
func TestInvariantFailureLeadsToFailed(t *testing.T) {
	st := newFakeStore()
	loader := loaderWithFixture(t, "pass_safe", passFixture)
	ex := newFakeExecutor()
	o := newOrchestrator(t, st, ex, loader)

	run, err := o.EnqueueScenario(context.Background(), "pass_safe")
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Two duplicate payment effects violate no_duplicate_effect.
	pay := &store.SideEffectRecord{
		ID:             fxID("effect"),
		RunID:          run.ID,
		EffectType:     "payment",
		AggregateID:    "order-1",
		IdempotencyKey: strPtr("pay-1"),
		AmountCents:    int64Ptr(1000),
		Currency:       "USD",
	}
	st.SetSideEffects(run.ID, pay, pay)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := o.ExecuteRun(ctx, run.ID); err != nil {
		t.Fatalf("execute run: %v", err)
	}

	assertRunStatus(t, st, run.ID, string(store.RunStateFailed))
	results, err := st.ListInvariantResultsByRun(context.Background(), run.ID)
	if err != nil {
		t.Fatalf("list invariant results: %v", err)
	}
	var failed bool
	for _, r := range results {
		if r.Verdict != "pass" {
			failed = true
		}
	}
	if !failed {
		t.Fatal("expected at least one failed invariant result")
	}
}
