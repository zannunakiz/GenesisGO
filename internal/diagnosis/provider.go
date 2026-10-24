// Package diagnosis provides optional local AI-assisted diagnosis for run
// reports. The reliability engine and deterministic verdicts never depend on
// an LLM: when a DiagnosisProvider is unavailable, runs and reports complete
// normally with diagnosis marked unavailable.
//
// All AI statements are hypotheses kept separate from deterministic facts.
// A provider never changes run state, invariant outcomes, or trigger extra
// execution.
package diagnosis

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// DiagnosisProvider generates an optional, validated explanation for a run.
// Implementations must treat all input as untrusted, apply timeouts and
// size bounds, and must never issue shell commands or change run state.
// A nil result with a nil error means diagnosis is unavailable (e.g. no
// provider configured) and must not fail the run or report.
type DiagnosisProvider interface {
	Diagnose(ctx context.Context, in Input) (*Result, error)
}

// Input carries only the minimum non-sensitive evidence needed for a local
// AI explanation. No raw event payloads or secrets are included.
type Input struct {
	// RunID and ScenarioID identify the run without exposing secrets.
	RunID      string `json:"run_id"`
	ScenarioID string `json:"scenario_id"`
	// OverallVerdict is the deterministic run verdict (pass/fail/not_evaluated).
	OverallVerdict string `json:"overall_verdict"`
	// FailedInvariants lists invariant keys and verdicts that failed.
	FailedInvariants []InvariantSummary `json:"failed_invariants,omitempty"`
	// EventTimeline is a concise ordered summary of delivery attempts.
	EventTimeline []EventTimelineEntry `json:"event_timeline,omitempty"`
	// FaultPoints records scenario-injected fault injection points.
	FaultPoints []FaultPointSummary `json:"fault_points,omitempty"`
	// StateDiffs captures before/after state summaries for affected steps.
	StateDiffs []StateDiffSummary `json:"state_diffs,omitempty"`
}

// InvariantSummary is a condensed view of a single invariant result.
type InvariantSummary struct {
	Key     string `json:"invariant_key"`
	Verdict string `json:"verdict"`
}

// EventTimelineEntry is a sanitized view of a delivery attempt.
type EventTimelineEntry struct {
	EventID        string     `json:"event_id"`
	AttemptNumber  int32      `json:"attempt_number"`
	DeliveredAt    time.Time  `json:"delivered_at"`
	AcknowledgedAt *time.Time `json:"acknowledged_at,omitempty"`
	FaultInjected  *string    `json:"fault_injected,omitempty"`
}

// FaultPointSummary records a scenario-injected fault point.
type FaultPointSummary struct {
	EventID       string    `json:"event_id"`
	AttemptNumber int32     `json:"attempt_number"`
	FaultType     string    `json:"fault_type"`
	DeliveredAt   time.Time `json:"delivered_at"`
}

// StateDiffSummary captures a safe before/after state summary.
type StateDiffSummary struct {
	StepID      string `json:"step_id"`
	EventID     string `json:"event_id"`
	AggregateID string `json:"aggregate_id"`
}

// Result is the provider's hypothesis. Every field is a hypothesis, never a
// ground-truth verdict: AI output can never override invariant pass/fail.
// Fields may be nil/empty when unavailable. The JSON shape of this struct is
// the diagnosis schema: summary, probable_causes, cited_evidence_ids,
// suggested_follow_up_scenarios, confidence_rationale, model, created_at.
type Result struct {
	// Summary is a one-paragraph hypothesis explaining the run outcome.
	Summary *string `json:"summary,omitempty"`
	// ProbableCauses lists hypothesized root causes for any failed invariants.
	ProbableCauses []string `json:"probable_causes,omitempty"`
	// CitedEvidenceIDs lists run evidence IDs the hypothesis references. Every
	// ID must correspond to real persisted evidence; fabricated references are
	// rejected by Validate.
	CitedEvidenceIDs []string `json:"cited_evidence_ids,omitempty"`
	// SuggestedFollowUpScenarios lists scenarios the operator may want to run next.
	SuggestedFollowUpScenarios []string `json:"suggested_follow_up_scenarios,omitempty"`
	// ConfidenceRationale explains why the model is confident (or not) in the hypothesis.
	ConfidenceRationale *string `json:"confidence_rationale,omitempty"`
	// Model is the model that produced the diagnosis.
	Model *string `json:"model,omitempty"`
	// CreatedAt records when the diagnosis was generated.
	CreatedAt time.Time `json:"created_at"`
}

// Validate checks the diagnosis result against the run's known evidence IDs.
// All AI output is treated as untrusted input: this method only inspects the
// data, it never executes or acts on it. It rejects fabricated evidence
// references (cited IDs that are not part of this run) and empty cited IDs.
// A nil result is invalid.
func (r *Result) Validate(knownEvidenceIDs []string) error {
	if r == nil {
		return errors.New("diagnosis result is required")
	}
	known := make(map[string]struct{}, len(knownEvidenceIDs))
	for _, id := range knownEvidenceIDs {
		known[id] = struct{}{}
	}
	for _, id := range r.CitedEvidenceIDs {
		if id == "" {
			return errors.New("cited evidence id must not be empty")
		}
		if _, ok := known[id]; !ok {
			return fmt.Errorf("cited evidence id %q is not part of this run", id)
		}
	}
	return nil
}
