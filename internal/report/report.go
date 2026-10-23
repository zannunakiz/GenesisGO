// Package report generates reproducible run reports from persisted evidence.
// A report combines run summary, scenario, event attempts, fault points,
// side-effect diff, invariant results, and safe error details into a single
// versioned JSON document. Diagnosis is kept as a separate optional section
// so deterministic facts remain distinguishable from AI hypotheses.
//
// All data is read from persisted store records, so a report can be
// regenerated from the same evidence at any time. Report output is
// deterministic apart from explicitly documented timestamps and identifiers.
// No raw event payloads or sensitive data are included.
package report

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/zannunakiz/GenesisGO/internal/store"
)

// SchemaVersion is the stable report schema version.
const SchemaVersion = "1.0.0"

// ReportStore is the subset of store.Store needed to generate a report.
type ReportStore interface {
	GetRun(ctx context.Context, id string) (*store.Run, error)
	GetScenario(ctx context.Context, id string) (*store.Scenario, error)
	ListEventAttemptsByRun(ctx context.Context, runID string) ([]*store.EventAttempt, error)
	ListSideEffectRecordsByRun(ctx context.Context, runID string) ([]*store.SideEffectRecord, error)
	ListStateSnapshotsByRun(ctx context.Context, runID string) ([]*store.StateSnapshot, error)
	ListInvariantResultsByRun(ctx context.Context, runID string) ([]*store.InvariantResult, error)
}
// Report is a versioned, reproducible run report.
type Report struct {
	SchemaVersion    string               `json:"schema_version"`
	Run              RunSummary           `json:"run"`
	Scenario         ScenarioSummary      `json:"scenario"`
	Timestamps       Timestamps           `json:"timestamps"`
	EventAttempts    []EventAttemptEntry  `json:"event_attempts"`
	FaultPoints      []FaultPointEntry    `json:"fault_points"`
	SideEffectDiff   []SnapshotPair       `json:"side_effect_diff"`
	InvariantResults []InvariantResultEntry `json:"invariant_results"`
	ErrorDetails     *ErrorDetails        `json:"error_details,omitempty"`
	Diagnosis        *DiagnosisSection    `json:"diagnosis,omitempty"`
}

// RunSummary summarizes a run without exposing sensitive fields.
type RunSummary struct {
	ID           string `json:"id"`
	ScenarioID   string `json:"scenario_id"`
	Status       string `json:"status"`
	AttemptCount int32  `json:"attempt_count"`
	Version      int32  `json:"version"`
}

// ScenarioSummary summarizes a scenario definition.
type ScenarioSummary struct {
	Key         string `json:"key"`
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
}

// Timestamps captures the key timing data for a run.
type Timestamps struct {
	CreatedAt   time.Time  `json:"created_at"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
	UpdatedAt   time.Time  `json:"updated_at"`
}

// EventAttemptEntry is a sanitized view of a delivery attempt.
type EventAttemptEntry struct {
	ID             string     `json:"id"`
	EventID        string     `json:"event_id"`
	AttemptNumber  int32      `json:"attempt_number"`
	DeliveredAt    time.Time  `json:"delivered_at"`
	AcknowledgedAt *time.Time `json:"acknowledged_at,omitempty"`
	FaultInjected  *string    `json:"fault_injected,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
}

// FaultPointEntry records a scenario-injected fault point.
type FaultPointEntry struct {
	StepID        string     `json:"step_id"`
	EventID       string     `json:"event_id"`
	AttemptNumber int32      `json:"attempt_number"`
	FaultType     string     `json:"fault_type"`
	DeliveredAt   time.Time  `json:"delivered_at"`
}

// SnapshotPair pairs before/after state snapshots for a single delivery.
type SnapshotPair struct {
	StepID      string             `json:"step_id"`
	EventID     string             `json:"event_id"`
	AggregateID string             `json:"aggregate_id"`
	Before      *StateSnapshotEntry `json:"before,omitempty"`
	After       *StateSnapshotEntry `json:"after,omitempty"`
}

// StateSnapshotEntry is a sanitized view of a state snapshot.
type StateSnapshotEntry struct {
	SnapshotType string    `json:"snapshot_type"`
	AggregateID  string    `json:"aggregate_id"`
	StateSummary []byte    `json:"state_summary,omitempty"`
	CreatedAt    time.Time `json:"created_at"`
}

// InvariantResultEntry is a sanitized view of an invariant verdict.
type InvariantResultEntry struct {
	InvariantKey string    `json:"invariant_key"`
	Rule         string    `json:"rule"`
	Verdict      string    `json:"verdict"`
	Expected     string    `json:"expected"`
	Actual       string    `json:"actual"`
	Evidence     []string  `json:"evidence"`
	EvaluatedAt  time.Time `json:"evaluated_at"`
}

// ErrorDetails contains safe, non-sensitive error information for a failed run.
type ErrorDetails struct {
	FailureReason      *string `json:"failure_reason,omitempty"`
	CancellationReason *string `json:"cancellation_reason,omitempty"`
}

// DiagnosisSection is an optional, clearly-labeled AI diagnosis section.
// It is separate from deterministic facts so reviewers can distinguish
// hypotheses from verified results.
type DiagnosisSection struct {
	Summary     *string `json:"summary,omitempty"`
	Suggestions []byte  `json:"suggestions,omitempty"`
	Model       *string `json:"model,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	Source      string  `json:"source"`
}

// GenerateReport builds a complete report for a run from persisted evidence.
// It returns an error if the run or scenario cannot be found.
func GenerateReport(ctx context.Context, st ReportStore, runID string) (*Report, error) {
	run, err := st.GetRun(ctx, runID)
	if err != nil {
		return nil, fmt.Errorf("get run: %w", err)
	}

	scenario, err := st.GetScenario(ctx, run.ScenarioID)
	if err != nil {
		return nil, fmt.Errorf("get scenario: %w", err)
	}

	attempts, err := st.ListEventAttemptsByRun(ctx, runID)
	if err != nil {
		return nil, fmt.Errorf("list event attempts: %w", err)
	}

	snapshots, err := st.ListStateSnapshotsByRun(ctx, runID)
	if err != nil {
		return nil, fmt.Errorf("list state snapshots: %w", err)
	}

	invariants, err := st.ListInvariantResultsByRun(ctx, runID)
	if err != nil {
		return nil, fmt.Errorf("list invariant results: %w", err)
	}

	// Build event attempt entries.
	attemptEntries := make([]EventAttemptEntry, 0, len(attempts))
	for _, a := range attempts {
		attemptEntries = append(attemptEntries, EventAttemptEntry{
			ID:             a.ID,
			EventID:        a.EventID,
			AttemptNumber:  a.AttemptNumber,
			DeliveredAt:    a.DeliveredAt,
			AcknowledgedAt: a.AcknowledgedAt,
			FaultInjected:  a.FaultInjected,
			CreatedAt:      a.CreatedAt,
		})
	}

	// Build fault point entries from attempts with injected faults.
	faultPoints := buildFaultPoints(attempts)

	// Build side-effect diff from snapshot pairs.
	sideEffectDiff := buildSnapshotPairs(snapshots)

	// Build invariant result entries.
	invariantEntries := make([]InvariantResultEntry, 0, len(invariants))
	for _, inv := range invariants {
		entry := InvariantResultEntry{
			InvariantKey: inv.InvariantKey,
			Verdict:      inv.Verdict,
			EvaluatedAt:  inv.EvaluatedAt,
		}
		if inv.Details != nil {
			var details map[string]any
			if err := json.Unmarshal(inv.Details, &details); err == nil {
				if rule, ok := details["rule"].(string); ok {
					entry.Rule = rule
				}
				if expected, ok := details["expected"].(string); ok {
					entry.Expected = expected
				}
				if actual, ok := details["actual"].(string); ok {
					entry.Actual = actual
				}
				if evidence, ok := details["evidence"].([]any); ok {
					for _, e := range evidence {
						if s, ok := e.(string); ok {
							entry.Evidence = append(entry.Evidence, s)
						}
					}
				}
			}
		}
		invariantEntries = append(invariantEntries, entry)
	}

	// Build error details from run failure/cancellation reasons.
	var errorDetails *ErrorDetails
	if run.FailureReason != nil || run.CancellationReason != nil {
		errorDetails = &ErrorDetails{
			FailureReason:      run.FailureReason,
			CancellationReason: run.CancellationReason,
		}
	}

	report := &Report{
		SchemaVersion:     SchemaVersion,
		Run: RunSummary{
			ID:           run.ID,
			ScenarioID:   run.ScenarioID,
			Status:       run.Status,
			AttemptCount: run.AttemptCount,
			Version:      run.Version,
		},
		Scenario: ScenarioSummary{
			Key:         scenario.Key,
			Name:        scenario.Name,
			Description: scenario.Description,
		},
		Timestamps: Timestamps{
			CreatedAt:   run.CreatedAt,
			StartedAt:   run.StartedAt,
			CompletedAt: run.CompletedAt,
			UpdatedAt:   run.UpdatedAt,
		},
		EventAttempts:   attemptEntries,
		FaultPoints:     faultPoints,
		SideEffectDiff:  sideEffectDiff,
		InvariantResults: invariantEntries,
		ErrorDetails:    errorDetails,
	}

	return report, nil
}

// buildFaultPoints extracts fault-injected attempts into fault point entries.
func buildFaultPoints(attempts []*store.EventAttempt) []FaultPointEntry {
	var faults []FaultPointEntry
	for _, a := range attempts {
		if a.FaultInjected == nil {
			continue
		}
		faults = append(faults, FaultPointEntry{
			EventID:        a.EventID,
			AttemptNumber:  a.AttemptNumber,
			FaultType:      *a.FaultInjected,
			DeliveredAt:    a.DeliveredAt,
		})
	}
	return faults
}

// buildSnapshotPairs pairs before/after state snapshots by step and event.
func buildSnapshotPairs(snapshots []*store.StateSnapshot) []SnapshotPair {
	type key struct {
		StepID  string
		EventID string
	}
	groups := make(map[key][]*store.StateSnapshot)
	for _, s := range snapshots {
		groups[key{StepID: s.StepID, EventID: s.EventID}] = append(groups[key{StepID: s.StepID, EventID: s.EventID}], s)
	}

	var keys []key
	seen := make(map[key]bool)
	for _, s := range snapshots {
		k := key{StepID: s.StepID, EventID: s.EventID}
		if !seen[k] {
			seen[k] = true
			keys = append(keys, k)
		}
	}

	pairs := make([]SnapshotPair, 0, len(keys))
	for _, k := range keys {
		group := groups[k]
		pair := SnapshotPair{
			StepID:      k.StepID,
			EventID:     k.EventID,
			AggregateID: group[0].AggregateID,
		}
		for _, s := range group {
			entry := &StateSnapshotEntry{
				SnapshotType: s.SnapshotType,
				AggregateID:  s.AggregateID,
				StateSummary: s.StateSummary,
				CreatedAt:    s.CreatedAt,
			}
			switch s.SnapshotType {
			case "before":
				pair.Before = entry
			case "after":
				pair.After = entry
			}
		}
		pairs = append(pairs, pair)
	}

	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].StepID != pairs[j].StepID {
			return pairs[i].StepID < pairs[j].StepID
		}
		return pairs[i].EventID < pairs[j].EventID
	})

	return pairs
}

// JSON serializes the report to deterministic JSON bytes.
func (r *Report) JSON() ([]byte, error) {
	return json.Marshal(r)
}

// Validate checks that the report is well-formed.
func (r *Report) Validate() error {
	if r.SchemaVersion == "" {
		return errors.New("schema_version is required")
	}
	if r.SchemaVersion != SchemaVersion {
		return fmt.Errorf("unsupported schema version %q", r.SchemaVersion)
	}
	if r.Run.ID == "" {
		return errors.New("run id is required")
	}
	if r.Scenario.Key == "" {
		return errors.New("scenario key is required")
	}
	return nil
}

// Ensure store.Store satisfies ReportStore.
var _ ReportStore = (*store.Store)(nil)
