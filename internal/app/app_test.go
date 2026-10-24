package app

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/zannunakiz/GenesisGO/internal/orchestrator"
	"github.com/zannunakiz/GenesisGO/internal/scenarios"
	"github.com/zannunakiz/GenesisGO/internal/store"
)

// fakeScenarioLoader returns a fixed set of built-in scenarios.
type fakeScenarioLoader struct {
	fixtures map[string]*scenarios.Scenario
	err      error
}

func (f *fakeScenarioLoader) Load() (map[string]*scenarios.Scenario, error) {
	return f.fixtures, f.err
}

// fakeRunStore keeps runs in memory for testing.
type fakeRunStore struct {
	runs    map[string]*store.Run
	getMu   map[string]struct{}
	counter int
}

func newFakeRunStore() *fakeRunStore {
	return &fakeRunStore{runs: make(map[string]*store.Run), getMu: make(map[string]struct{})}
}

func (f *fakeRunStore) GetRun(ctx context.Context, id string) (*store.Run, error) {
	_ = ctx
	f.getMu[id] = struct{}{}
	run, ok := f.runs[id]
	if !ok {
		return nil, pgxErrNoRows
	}
	return run, nil
}

func (f *fakeRunStore) ListRuns(ctx context.Context) ([]*store.Run, error) {
	_ = ctx
	out := make([]*store.Run, 0, len(f.runs))
	for _, r := range f.runs {
		out = append(out, r)
	}
	return out, nil
}

func (f *fakeRunStore) ListRunsByScenario(ctx context.Context, scenarioID string) ([]*store.Run, error) {
	_ = ctx
	var out []*store.Run
	for _, r := range f.runs {
		if r.ScenarioID == scenarioID {
			out = append(out, r)
		}
	}
	return out, nil
}

func (f *fakeRunStore) addRun(key, status string) *store.Run {
	f.counter++
	run := &store.Run{
		ID:          fmt.Sprintf("run-%s-%d", key, f.counter),
		ScenarioID:  key,
		Status:      status,
		CreatedAt:   time.Now().UTC(),
		AttemptCount: 1,
		Version:     1,
	}
	f.runs[run.ID] = run
	return run
}

var pgxErrNoRows = errors.New("no rows in result set")

// fakeRunCreator creates runs via the orchestrator interface and mirrors them
// into a fakeRunStore so GetRun can find them (simulating the real store).
type fakeRunCreator struct {
	store *fakeRunStore
	err   error
	count int
}

func (f *fakeRunCreator) EnqueueScenario(ctx context.Context, scenarioKey string) (*store.Run, error) {
	_ = ctx
	f.count++
	if f.err != nil {
		return nil, f.err
	}
	if scenarioKey == "missing" {
		return nil, orchestrator.ErrScenarioNotFound
	}
	run := &store.Run{
		ID:          fmt.Sprintf("run-%s-%d", scenarioKey, f.count),
		ScenarioID:  scenarioKey,
		Status:      string(store.RunStatePending),
		CreatedAt:   time.Now().UTC(),
		AttemptCount: 1,
		Version:     1,
	}
	if f.store != nil {
		f.store.runs[run.ID] = run
	}
	return run, nil
}

// fakeRunCanceler cancels runs by updating their status in a fakeRunStore.
type fakeRunCanceler struct {
	store  *fakeRunStore
	err    error
	cancel map[string]struct{} // tracks cancelled run IDs
}

func newFakeRunCanceler(st *fakeRunStore) *fakeRunCanceler {
	return &fakeRunCanceler{store: st, cancel: make(map[string]struct{})}
}

func (f *fakeRunCanceler) CancelRun(ctx context.Context, runID string) (*store.Run, error) {
	_ = ctx
	if f.err != nil {
		return nil, f.err
	}
	run, ok := f.store.runs[runID]
	if !ok {
		return nil, pgxErrNoRows
	}
	if store.IsTerminal(store.RunState(run.Status)) {
		return nil, fmt.Errorf("%w: run %q is already terminal (%q)", store.ErrInvalidTransition, runID, run.Status)
	}
	run.Status = string(store.RunStateCancelled)
	run.CancellationReason = strPtr("cancelled via API")
	f.cancel[runID] = struct{}{}
	return run, nil
}

func strPtr(s string) *string { return &s }

func TestListScenariosReturnsSortedSummaries(t *testing.T) {
	loader := &fakeScenarioLoader{
		fixtures: map[string]*scenarios.Scenario{
			"zeta": {Key: "zeta", Version: 2, Name: "Zeta Scenario", Description: "desc z"},
			"alpha": {Key: "alpha", Version: 1, Name: "Alpha Scenario", Description: "desc a"},
		},
	}
	uc := NewUseCases(loader, newFakeRunStore(), &fakeRunCreator{}, newFakeRunCanceler(newFakeRunStore()))

	list, err := uc.ListScenarios(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("expected 2 scenarios, got %d", len(list))
	}
	if list[0].Key != "alpha" || list[1].Key != "zeta" {
		t.Fatalf("expected sorted by key, got %v", list)
	}
}

func TestListScenariosPropagatesLoaderError(t *testing.T) {
	loader := &fakeScenarioLoader{err: errors.New("load failed")}
	uc := NewUseCases(loader, newFakeRunStore(), &fakeRunCreator{}, newFakeRunCanceler(newFakeRunStore()))

	if _, err := uc.ListScenarios(context.Background()); err == nil {
		t.Fatal("expected loader error to propagate")
	}
}

func TestCreateRunCreatesNewRun(t *testing.T) {
	st := newFakeRunStore()
	creator := &fakeRunCreator{store: st}
	canceler := newFakeRunCanceler(st)
	uc := NewUseCases(&fakeScenarioLoader{}, st, creator, canceler)

	run, created, err := uc.CreateRun(context.Background(), CreateRunInput{ScenarioKey: "duplicate_delivery"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !created {
		t.Fatal("expected created=true for new run")
	}
	if run.ScenarioID != "duplicate_delivery" {
		t.Fatalf("expected scenario_id duplicate_delivery, got %q", run.ScenarioID)
	}
	if run.Status != string(store.RunStatePending) {
		t.Fatalf("expected pending status, got %q", run.Status)
	}
}

func TestCreateRunRejectsUnknownScenario(t *testing.T) {
	st := newFakeRunStore()
	creator := &fakeRunCreator{store: st}
	canceler := newFakeRunCanceler(st)
	uc := NewUseCases(&fakeScenarioLoader{}, st, creator, canceler)

	_, _, err := uc.CreateRun(context.Background(), CreateRunInput{ScenarioKey: "missing"})
	if !errors.Is(err, orchestrator.ErrScenarioNotFound) {
		t.Fatalf("expected ErrScenarioNotFound, got %v", err)
	}
}

func TestCreateRunIdempotencyKeyReturnsSameRun(t *testing.T) {
	st := newFakeRunStore()
	creator := &fakeRunCreator{store: st}
	canceler := newFakeRunCanceler(st)
	uc := NewUseCases(&fakeScenarioLoader{}, st, creator, canceler)

	run1, created1, err := uc.CreateRun(context.Background(), CreateRunInput{
		ScenarioKey:    "duplicate_delivery",
		IdempotencyKey: "key-1",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !created1 {
		t.Fatal("expected created=true for first request")
	}

	run2, created2, err := uc.CreateRun(context.Background(), CreateRunInput{
		ScenarioKey:    "duplicate_delivery",
		IdempotencyKey: "key-1",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if created2 {
		t.Fatal("expected created=false for duplicate idempotency key")
	}
	if run2.ID != run1.ID {
		t.Fatalf("expected same run id, got %q and %q", run2.ID, run1.ID)
	}
	if creator.count != 1 {
		t.Fatalf("expected EnqueueScenario called once, got %d", creator.count)
	}
}

func TestCreateRunWithoutIdempotencyKeyCreatesNewRunEachTime(t *testing.T) {
	st := newFakeRunStore()
	creator := &fakeRunCreator{store: st}
	canceler := newFakeRunCanceler(st)
	uc := NewUseCases(&fakeScenarioLoader{}, st, creator, canceler)

	_, c1, err := uc.CreateRun(context.Background(), CreateRunInput{ScenarioKey: "duplicate_delivery"})
	if err != nil || !c1 {
		t.Fatalf("expected created run, got c1=%v err=%v", c1, err)
	}
	_, c2, err := uc.CreateRun(context.Background(), CreateRunInput{ScenarioKey: "duplicate_delivery"})
	if err != nil || !c2 {
		t.Fatalf("expected created run on second request, got c2=%v err=%v", c2, err)
	}
	if creator.count != 2 {
		t.Fatalf("expected EnqueueScenario called twice, got %d", creator.count)
	}
}

func TestListRunsPaginates(t *testing.T) {
	st := newFakeRunStore()
	for i := 1; i <= 5; i++ {
		st.addRun("scenario-a", string(store.RunStatePending))
	}
	uc := NewUseCases(&fakeScenarioLoader{}, st, &fakeRunCreator{}, newFakeRunCanceler(st))

	list, total, err := uc.ListRuns(context.Background(), ListRunsInput{
		ScenarioID: "scenario-a",
		Limit:      2,
		Offset:     1,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if total != 5 {
		t.Fatalf("expected total 5, got %d", total)
	}
	if len(list) != 2 {
		t.Fatalf("expected 2 items on page, got %d", len(list))
	}
}

func TestListRunsRespectsMaxLimit(t *testing.T) {
	st := newFakeRunStore()
	for i := 1; i <= 5; i++ {
		st.addRun("scenario-a", string(store.RunStatePending))
	}
	uc := NewUseCases(&fakeScenarioLoader{}, st, &fakeRunCreator{}, newFakeRunCanceler(st))

	list, _, err := uc.ListRuns(context.Background(), ListRunsInput{
		ScenarioID: "scenario-a",
		Limit:      999,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(list) != 5 {
		t.Fatalf("expected all 5 runs (limit capped), got %d", len(list))
	}
}

func TestListRunsWithoutScenarioFilterListsAll(t *testing.T) {
	st := newFakeRunStore()
	st.addRun("scenario-a", string(store.RunStatePending))
	st.addRun("scenario-b", string(store.RunStatePending))
	uc := NewUseCases(&fakeScenarioLoader{}, st, &fakeRunCreator{}, newFakeRunCanceler(st))

	list, total, err := uc.ListRuns(context.Background(), ListRunsInput{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if total != 2 {
		t.Fatalf("expected total 2, got %d", total)
	}
	if len(list) != 2 {
		t.Fatalf("expected 2 runs, got %d", len(list))
	}
}

func TestGetRunReturnsRun(t *testing.T) {
	st := newFakeRunStore()
	run := st.addRun("scenario-a", string(store.RunStatePending))
	uc := NewUseCases(&fakeScenarioLoader{}, st, &fakeRunCreator{}, newFakeRunCanceler(st))

	got, err := uc.GetRun(context.Background(), run.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.ID != run.ID {
		t.Fatalf("expected run id %q, got %q", run.ID, got.ID)
	}
}

func TestGetRunReturnsNotFoundForMissing(t *testing.T) {
	st := newFakeRunStore()
	uc := NewUseCases(&fakeScenarioLoader{}, st, &fakeRunCreator{}, newFakeRunCanceler(st))

	if _, err := uc.GetRun(context.Background(), "no-such-run"); !errors.Is(err, pgxErrNoRows) {
		t.Fatalf("expected pgxErrNoRows, got %v", err)
	}
}

func TestCancelRunBeforeStart(t *testing.T) {
	st := newFakeRunStore()
	run := st.addRun("scenario-a", string(store.RunStatePending))
	creator := &fakeRunCreator{store: st}
	canceler := newFakeRunCanceler(st)
	uc := NewUseCases(&fakeScenarioLoader{}, st, creator, canceler)

	cancelled, err := uc.CancelRun(context.Background(), run.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cancelled.Status != string(store.RunStateCancelled) {
		t.Fatalf("expected cancelled status, got %q", cancelled.Status)
	}
	if cancelled.CancellationReason == nil || *cancelled.CancellationReason == "" {
		t.Fatal("expected cancellation reason to be set")
	}
	if _, ok := canceler.cancel[run.ID]; !ok {
		t.Fatal("expected cancel to be recorded")
	}
}

func TestCancelRunDuringRun(t *testing.T) {
	st := newFakeRunStore()
	run := st.addRun("scenario-a", string(store.RunStateRunning))
	creator := &fakeRunCreator{store: st}
	canceler := newFakeRunCanceler(st)
	uc := NewUseCases(&fakeScenarioLoader{}, st, creator, canceler)

	cancelled, err := uc.CancelRun(context.Background(), run.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cancelled.Status != string(store.RunStateCancelled) {
		t.Fatalf("expected cancelled status, got %q", cancelled.Status)
	}
}

func TestCancelRunAlreadyTerminal(t *testing.T) {
	st := newFakeRunStore()
	run := st.addRun("scenario-a", string(store.RunStateCompleted))
	creator := &fakeRunCreator{store: st}
	canceler := newFakeRunCanceler(st)
	uc := NewUseCases(&fakeScenarioLoader{}, st, creator, canceler)

	if _, err := uc.CancelRun(context.Background(), run.ID); !errors.Is(err, store.ErrInvalidTransition) {
		t.Fatalf("expected ErrInvalidTransition for terminal run, got %v", err)
	}
}

func TestCancelRunNotFound(t *testing.T) {
	st := newFakeRunStore()
	creator := &fakeRunCreator{store: st}
	canceler := newFakeRunCanceler(st)
	uc := NewUseCases(&fakeScenarioLoader{}, st, creator, canceler)

	if _, err := uc.CancelRun(context.Background(), "no-such-run"); !errors.Is(err, pgxErrNoRows) {
		t.Fatalf("expected pgxErrNoRows for missing run, got %v", err)
	}
}