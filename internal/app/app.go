// Package app provides application use cases that orchestrate domain services
// for the REST API. Handlers are thin and delegate to these use cases.
package app

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/zannunakiz/GenesisGO/internal/report"
	"github.com/zannunakiz/GenesisGO/internal/scenarios"
	"github.com/zannunakiz/GenesisGO/internal/store"
)

// ScenarioSummary is a safe, minimal view of a built-in scenario.
type ScenarioSummary struct {
	Key         string `json:"key"`
	Version     int    `json:"version"`
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
}

// RunSummary is a safe, minimal view of a run for API responses.
type RunSummary struct {
	ID           string     `json:"id"`
	ScenarioID   string     `json:"scenario_id"`
	Status       string     `json:"status"`
	CreatedAt    time.Time  `json:"created_at"`
	StartedAt    *time.Time `json:"started_at,omitempty"`
	CompletedAt  *time.Time `json:"completed_at,omitempty"`
	AttemptCount int32      `json:"attempt_count"`
}

// newRunSummary converts a store.Run into a safe API summary.
func newRunSummary(r *store.Run) RunSummary {
	return RunSummary{
		ID:           r.ID,
		ScenarioID:   r.ScenarioID,
		Status:       r.Status,
		CreatedAt:    r.CreatedAt,
		StartedAt:    r.StartedAt,
		CompletedAt:  r.CompletedAt,
		AttemptCount: r.AttemptCount,
	}
}

// scenarioLister loads built-in scenarios.
type scenarioLister interface {
	Load() (map[string]*scenarios.Scenario, error)
}

// runStore is the subset of store.Store needed by run use cases.
type runStore interface {
	GetRun(ctx context.Context, id string) (*store.Run, error)
	ListRuns(ctx context.Context) ([]*store.Run, error)
	ListRunsByScenario(ctx context.Context, scenarioID string) ([]*store.Run, error)
}

// runCreator creates runs.
type runCreator interface {
	EnqueueScenario(ctx context.Context, scenarioKey string) (*store.Run, error)
}

// cancelRunner cancels a running or pending run.
type cancelRunner interface {
	CancelRun(ctx context.Context, runID string) (*store.Run, error)
}

// reportGenerator generates a reproducible run report from persisted evidence.
type reportGenerator interface {
	GenerateReport(ctx context.Context, runID string) (*report.Report, error)
}

// idempotencyStore is a process-scoped cache for run creation idempotency keys.
type idempotencyStore struct {
	mu sync.Mutex
	m  map[string]string // idempotency key -> run ID
}

func newIdempotencyStore() *idempotencyStore {
	return &idempotencyStore{m: make(map[string]string)}
}

func (s *idempotencyStore) Get(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	runID, ok := s.m[key]
	return runID, ok
}

func (s *idempotencyStore) Set(key, runID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[key] = runID
}

// UseCases groups all application use cases exposed through the REST API.
type UseCases struct {
	scenarios   scenarioLister
	runs        runStore
	creator     runCreator
	canceler    cancelRunner
	reportGen   reportGenerator
	idempotency *idempotencyStore
	audit       auditLogger
}

// auditLogger records security-relevant actions. Detail is treated as a
// redacted summary and must never contain credentials.
type auditLogger interface {
	LogAudit(ctx context.Context, runID, action, actorKeyID, detail string) error
}

// NewUseCases builds application use cases from their dependencies.
func NewUseCases(scenarios scenarioLister, runs runStore, creator runCreator, canceler cancelRunner, reportGen reportGenerator) *UseCases {
	return &UseCases{
		scenarios:   scenarios,
		runs:        runs,
		creator:     creator,
		canceler:    canceler,
		reportGen:   reportGen,
		idempotency: newIdempotencyStore(),
	}
}

// WithAuditLogger attaches an audit logger to the use cases. When nil,
// auditing is a no-op and audit failures never disrupt business operations.
func (u *UseCases) WithAuditLogger(logger auditLogger) {
	u.audit = logger
}

// logAudit records a security-relevant action. Failures are logged but never
// returned to callers: auditing is best-effort.
func (u *UseCases) LogAudit(ctx context.Context, runID, action, actorKeyID, detail string) error {
	if u.audit == nil {
		return nil
	}
	// Auditing is best-effort: failures are not propagated to callers so they
	// never disrupt the business operation.
	return u.audit.LogAudit(ctx, runID, action, actorKeyID, detail)
}

// ListScenarios returns all built-in scenarios sorted by key.
func (u *UseCases) ListScenarios(ctx context.Context) ([]ScenarioSummary, error) {
	_ = ctx
	fixtures, err := u.scenarios.Load()
	if err != nil {
		return nil, err
	}
	out := make([]ScenarioSummary, 0, len(fixtures))
	for _, s := range fixtures {
		out = append(out, ScenarioSummary{
			Key:         s.Key,
			Version:     s.Version,
			Name:        s.Name,
			Description: s.Description,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

// CreateRunInput is the input to create a run.
type CreateRunInput struct {
	ScenarioKey    string
	IdempotencyKey string
}

// CreateRun creates a run for a scenario with optional idempotency.
// When an idempotency key is provided and was already used, the previously
// created run is returned without creating a duplicate. The boolean reports
// whether a new run was created (true) or an existing run was returned (false).
func (u *UseCases) CreateRun(ctx context.Context, in CreateRunInput) (*store.Run, bool, error) {
	if in.IdempotencyKey != "" {
		if runID, ok := u.idempotency.Get(in.IdempotencyKey); ok {
			run, err := u.runs.GetRun(ctx, runID)
			if err != nil {
				return nil, false, err
			}
			return run, false, nil
		}
	}

	run, err := u.creator.EnqueueScenario(ctx, in.ScenarioKey)
	if err != nil {
		return nil, false, err
	}
	if in.IdempotencyKey != "" {
		u.idempotency.Set(in.IdempotencyKey, run.ID)
	}
	return run, true, nil
}

// ListRunsInput is the input to list runs.
type ListRunsInput struct {
	ScenarioID string
	Limit      int
	Offset     int
}

// ListRuns returns runs for a scenario with pagination, or all runs when
// scenario ID is empty. Returns the total count and the requested page.
func (u *UseCases) ListRuns(ctx context.Context, in ListRunsInput) ([]RunSummary, int, error) {
	_ = ctx
	var runs []*store.Run
	var err error
	if in.ScenarioID == "" {
		runs, err = u.runs.ListRuns(ctx)
	} else {
		runs, err = u.runs.ListRunsByScenario(ctx, in.ScenarioID)
	}
	if err != nil {
		return nil, 0, err
	}
	total := len(runs)
	if in.Offset > total {
		in.Offset = total
	}
	page := runs[in.Offset:]
	if in.Limit > 0 && len(page) > in.Limit {
		page = page[:in.Limit]
	}
	out := make([]RunSummary, 0, len(page))
	for _, r := range page {
		out = append(out, newRunSummary(r))
	}
	return out, total, nil
}

// GetRun returns a run by ID.
func (u *UseCases) GetRun(ctx context.Context, id string) (*store.Run, error) {
	return u.runs.GetRun(ctx, id)
}

// CancelRun cancels a queued or running run. It returns the updated run with
// status set to cancelled and the cancellation reason persisted. Calling cancel
// on an already-terminal run returns an error.
func (u *UseCases) CancelRun(ctx context.Context, id string) (*store.Run, error) {
	return u.canceler.CancelRun(ctx, id)
}

// GetReport returns a reproducible, versioned run report regenerated from
// persisted evidence. The report is deterministic apart from explicitly
// documented timestamps and identifiers, and contains no sensitive data.
func (u *UseCases) GetReport(ctx context.Context, runID string) (*report.Report, error) {
	return u.reportGen.GenerateReport(ctx, runID)
}

// Sentinel errors returned by use cases.
var (
	// ErrInvalidScenarioKey is returned when the scenario key is empty.
	ErrInvalidScenarioKey = errors.New("scenario key is required")
)
