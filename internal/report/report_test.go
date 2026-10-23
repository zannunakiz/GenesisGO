package report

import (
	"context"
	"encoding/json"
	"sort"
	"testing"
	"time"

	"github.com/zannunakiz/GenesisGO/internal/invariants"
	"github.com/zannunakiz/GenesisGO/internal/store"
)

// fakeReportStore implements ReportStore in memory for report unit tests.
type fakeReportStore struct {
	runs       map[string]*store.Run
	scenarios  map[string]*store.Scenario
	attempts   map[string][]*store.EventAttempt
	snapshots  map[string][]*store.StateSnapshot
	invariants map[string][]*store.InvariantResult
}

func newFakeReportStore() *fakeReportStore {
	return &fakeReportStore{
		runs:      make(map[string]*store.Run),
		scenarios: make(map[string]*store.Scenario),
		attempts:  make(map[string][]*store.EventAttempt),
		snapshots: make(map[string][]*store.StateSnapshot),
		invariants: make(map[string][]*store.InvariantResult),
	}
}

func (f *fakeReportStore) GetRun(ctx context.Context, id string) (*store.Run, error) {
	r, ok := f.runs[id]
	if !ok {
		return nil, store.ErrRunNotFound
	}
	return r, nil
}

func (f *fakeReportStore) GetScenario(ctx context.Context, id string) (*store.Scenario, error) {
	s, ok := f.scenarios[id]
	if !ok {
		return nil, store.ErrRunNotFound
	}
	return s, nil
}

func (f *fakeReportStore) ListEventAttemptsByRun(ctx context.Context, runID string) ([]*store.EventAttempt, error) {
	return f.attempts[runID], nil
}

func (f *fakeReportStore) ListSideEffectRecordsByRun(ctx context.Context, runID string) ([]*store.SideEffectRecord, error) {
	return nil, nil
}

func (f *fakeReportStore) ListStateSnapshotsByRun(ctx context.Context, runID string) ([]*store.StateSnapshot, error) {
	return f.snapshots[runID], nil
}

func (f *fakeReportStore) ListInvariantResultsByRun(ctx context.Context, runID string) ([]*store.InvariantResult, error) {
	return f.invariants[runID], nil
}

func TestGenerateReport(t *testing.T) {
	st := newFakeReportStore()
	ctx := context.Background()

	now := time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)

	runID := "run-123"
	scenarioID := "scenario-456"

	st.scenarios[scenarioID] = &store.Scenario{
		ID:          scenarioID,
		Key:         "duplicate_delivery",
		Name:        "Duplicate Delivery",
		Description: "A duplicate delivery test scenario",
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	startedAt := now.Add(time.Minute)
	completedAt := now.Add(2 * time.Minute)

	st.runs[runID] = &store.Run{
		ID:             runID,
		ScenarioID:     scenarioID,
		Status:         string(store.RunStateCompleted),
		CreatedAt:      now,
		StartedAt:      &startedAt,
		CompletedAt:    &completedAt,
		UpdatedAt:      completedAt,
		AttemptCount:   1,
		Version:        1,
	}

	st.attempts[runID] = []*store.EventAttempt{
		{
			ID:             "attempt-1",
			RunID:          runID,
			EventID:        "evt-001",
			AttemptNumber:  1,
			DeliveredAt:    now.Add(30 * time.Second),
			AcknowledgedAt: timePtr(now.Add(30 * time.Second)),
			CreatedAt:      now.Add(30 * time.Second),
		},
		{
			ID:             "attempt-2",
			RunID:          runID,
			EventID:        "evt-001",
			AttemptNumber:  2,
			DeliveredAt:    now.Add(45 * time.Second),
			FaultInjected:  strPtr("crash_after_commit"),
			CreatedAt:      now.Add(45 * time.Second),
		},
	}

	st.snapshots[runID] = []*store.StateSnapshot{
		{
			ID:           "snap-1",
			RunID:        runID,
			StepID:       "step-1",
			EventID:      "evt-001",
			SnapshotType: "before",
			AggregateID:  "order-100",
			StateSummary: []byte(`{"effect_count":0}`),
			CreatedAt:    now.Add(30 * time.Second),
		},
		{
			ID:           "snap-2",
			RunID:        runID,
			StepID:       "step-1",
			EventID:      "evt-001",
			SnapshotType: "after",
			AggregateID:  "order-100",
			StateSummary: []byte(`{"effect_count":1}`),
			CreatedAt:    now.Add(30 * time.Second),
		},
	}

	details, _ := json.Marshal(map[string]any{
		"rule":     "no_duplicate_effect",
		"expected": "at most one effect per (effect_type, aggregate_id)",
		"actual":   "no duplicates found",
		"evidence": []string{"effect-1"},
	})

	st.invariants[runID] = []*store.InvariantResult{
		{
			ID:           "inv-1",
			RunID:        runID,
			InvariantKey: "no_duplicate_payment",
			Verdict:      string(invariants.StatusPass),
			Details:      details,
			EvaluatedAt:  completedAt,
		},
	}

	rep, err := GenerateReport(ctx, st, runID)
	if err != nil {
		t.Fatalf("GenerateReport: %v", err)
	}

	if err := rep.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	if rep.SchemaVersion != SchemaVersion {
		t.Fatalf("expected schema version %q, got %q", SchemaVersion, rep.SchemaVersion)
	}
	if rep.Run.ID != runID {
		t.Fatalf("expected run id %q, got %q", runID, rep.Run.ID)
	}
	if rep.Run.Status != string(store.RunStateCompleted) {
		t.Fatalf("expected status %q, got %q", store.RunStateCompleted, rep.Run.Status)
	}
	if rep.Scenario.Key != "duplicate_delivery" {
		t.Fatalf("expected scenario key duplicate_delivery, got %q", rep.Scenario.Key)
	}
	if len(rep.EventAttempts) != 2 {
		t.Fatalf("expected 2 event attempts, got %d", len(rep.EventAttempts))
	}
	if len(rep.FaultPoints) != 1 {
		t.Fatalf("expected 1 fault point, got %d", len(rep.FaultPoints))
	}
	if rep.FaultPoints[0].FaultType != "crash_after_commit" {
		t.Fatalf("expected fault type crash_after_commit, got %q", rep.FaultPoints[0].FaultType)
	}
	if len(rep.SideEffectDiff) != 1 {
		t.Fatalf("expected 1 snapshot pair, got %d", len(rep.SideEffectDiff))
	}
	if rep.SideEffectDiff[0].Before == nil || rep.SideEffectDiff[0].After == nil {
		t.Fatal("expected both before and after snapshots")
	}
	if len(rep.InvariantResults) != 1 {
		t.Fatalf("expected 1 invariant result, got %d", len(rep.InvariantResults))
	}
	if rep.InvariantResults[0].Verdict != string(invariants.StatusPass) {
		t.Fatalf("expected verdict pass, got %q", rep.InvariantResults[0].Verdict)
	}
	if rep.ErrorDetails != nil {
		t.Fatal("expected no error details for completed run")
	}
}

func TestGenerateReportDeterministic(t *testing.T) {
	st := newFakeReportStore()
	ctx := context.Background()

	runID := "run-deterministic"
	scenarioID := "scenario-deterministic"

	now := time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)
	startedAt := now.Add(time.Minute)
	completedAt := now.Add(2 * time.Minute)

	st.scenarios[scenarioID] = &store.Scenario{
		ID:          scenarioID,
		Key:         "test_scenario",
		Name:        "Test",
		Description: "test",
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	st.runs[runID] = &store.Run{
		ID:             runID,
		ScenarioID:     scenarioID,
		Status:         string(store.RunStateCompleted),
		CreatedAt:      now,
		StartedAt:      &startedAt,
		CompletedAt:    &completedAt,
		UpdatedAt:      completedAt,
		AttemptCount:   1,
		Version:        1,
	}

	st.attempts[runID] = []*store.EventAttempt{
		{
			ID:            "attempt-1",
			RunID:         runID,
			EventID:       "evt-001",
			AttemptNumber: 1,
			DeliveredAt:   now.Add(30 * time.Second),
			CreatedAt:     now.Add(30 * time.Second),
		},
	}

	st.invariants[runID] = []*store.InvariantResult{
		{
			ID:           "inv-1",
			RunID:        runID,
			InvariantKey: "no_duplicate_effect",
			Verdict:      string(invariants.StatusPass),
			EvaluatedAt:  completedAt,
		},
	}

	rep1, err := GenerateReport(ctx, st, runID)
	if err != nil {
		t.Fatalf("GenerateReport 1: %v", err)
	}
	rep2, err := GenerateReport(ctx, st, runID)
	if err != nil {
		t.Fatalf("GenerateReport 2: %v", err)
	}

	j1, err := rep1.JSON()
	if err != nil {
		t.Fatalf("JSON 1: %v", err)
	}
	j2, err := rep2.JSON()
	if err != nil {
		t.Fatalf("JSON 2: %v", err)
	}

	if string(j1) != string(j2) {
		t.Fatal("reports are not deterministic")
	}
}

func TestGenerateReportMissingRun(t *testing.T) {
	st := newFakeReportStore()
	ctx := context.Background()

	_, err := GenerateReport(ctx, st, "nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent run")
	}
}

func TestGenerateReportNoSensitiveData(t *testing.T) {
	st := newFakeReportStore()
	ctx := context.Background()

	runID := "run-clean"
	scenarioID := "scenario-clean"

	now := time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)

	st.scenarios[scenarioID] = &store.Scenario{
		ID:          scenarioID,
		Key:         "test",
		Name:        "Test",
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	st.runs[runID] = &store.Run{
		ID:          runID,
		ScenarioID:  scenarioID,
		Status:      string(store.RunStateCompleted),
		CreatedAt:   now,
		UpdatedAt:   now,
		AttemptCount: 1,
		Version:     1,
	}

	st.attempts[runID] = []*store.EventAttempt{
		{
			ID:            "attempt-1",
			RunID:         runID,
			EventID:       "evt-001",
			AttemptNumber: 1,
			DeliveredAt:   now,
			CreatedAt:     now,
		},
	}

	rep, err := GenerateReport(ctx, st, runID)
	if err != nil {
		t.Fatalf("GenerateReport: %v", err)
	}

	j, err := rep.JSON()
	if err != nil {
		t.Fatalf("JSON: %v", err)
	}

	// Verify no raw payload or sensitive fields leak into the JSON.
	jsonStr := string(j)
	for _, sensitive := range []string{"password", "secret", "api_key", "token", "raw", "payload"} {
		if containsStr(jsonStr, sensitive) {
			t.Fatalf("report contains sensitive term %q: %s", sensitive, jsonStr)
		}
	}
}

func TestAllVerdictStates(t *testing.T) {
	st := newFakeReportStore()
	ctx := context.Background()

	runID := "run-verdicts"
	scenarioID := "scenario-verdicts"

	now := time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)

	st.scenarios[scenarioID] = &store.Scenario{
		ID:          scenarioID,
		Key:         "test",
		Name:        "Test",
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	st.runs[runID] = &store.Run{
		ID:          runID,
		ScenarioID:  scenarioID,
		Status:      string(store.RunStateCompleted),
		CreatedAt:   now,
		UpdatedAt:   now,
		AttemptCount: 1,
		Version:     1,
	}

	st.invariants[runID] = []*store.InvariantResult{
		{
			ID:           "inv-pass",
			RunID:        runID,
			InvariantKey: "no_duplicate_payment",
			Verdict:      string(invariants.StatusPass),
			EvaluatedAt:  now,
		},
		{
			ID:           "inv-fail",
			RunID:        runID,
			InvariantKey: "no_negative_inventory",
			Verdict:      string(invariants.StatusFail),
			EvaluatedAt:  now,
		},
		{
			ID:           "inv-not-eval",
			RunID:        runID,
			InvariantKey: "allowed_order_transition",
			Verdict:      string(invariants.StatusNotEvaluated),
			EvaluatedAt:  now,
		},
	}

	rep, err := GenerateReport(ctx, st, runID)
	if err != nil {
		t.Fatalf("GenerateReport: %v", err)
	}

	verdicts := make(map[string]bool)
	for _, inv := range rep.InvariantResults {
		verdicts[inv.Verdict] = true
	}

	if !verdicts[string(invariants.StatusPass)] {
		t.Fatal("expected pass verdict")
	}
	if !verdicts[string(invariants.StatusFail)] {
		t.Fatal("expected fail verdict")
	}
	if !verdicts[string(invariants.StatusNotEvaluated)] {
		t.Fatal("expected not_evaluated verdict")
	}
}

func strPtr(s string) *string { return &s }

func timePtr(t time.Time) *time.Time { return &t }

func containsStr(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

var _ = sort.Strings

