package httpapi

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/zannunakiz/GenesisGO/internal/app"
	"github.com/zannunakiz/GenesisGO/internal/auth"
	"github.com/zannunakiz/GenesisGO/internal/invariants"
	"github.com/zannunakiz/GenesisGO/internal/orchestrator"
	"github.com/zannunakiz/GenesisGO/internal/report"
	"github.com/zannunakiz/GenesisGO/internal/sse"
	"github.com/zannunakiz/GenesisGO/internal/store"
)

// fakeUseCases implements app use cases with controlled behavior for tests.
type fakeUseCases struct {
	scenarios    []app.ScenarioSummary
	scenErr      error
	createRun    *store.Run
	createErr    error
	created      bool
	listRuns     []app.RunSummary
	listTotal    int
	listErr      error
	getRun       *store.Run
	getErr       error
	cancelRun    *store.Run
	cancelErr    error
	cancelID     string
	report       *report.Report
	reportErr    error
	reportID     string
	auditEntries []auditEntry
}

func (f *fakeUseCases) ListScenarios(ctx context.Context) ([]app.ScenarioSummary, error) {
	return f.scenarios, f.scenErr
}

func (f *fakeUseCases) CreateRun(ctx context.Context, in app.CreateRunInput) (*store.Run, bool, error) {
	return f.createRun, f.created, f.createErr
}

func (f *fakeUseCases) ListRuns(ctx context.Context, in app.ListRunsInput) ([]app.RunSummary, int, error) {
	return f.listRuns, f.listTotal, f.listErr
}

func (f *fakeUseCases) GetRun(ctx context.Context, id string) (*store.Run, error) {
	return f.getRun, f.getErr
}

func (f *fakeUseCases) CancelRun(ctx context.Context, id string) (*store.Run, error) {
	f.cancelID = id
	return f.cancelRun, f.cancelErr
}

func (f *fakeUseCases) GetReport(ctx context.Context, runID string) (*report.Report, error) {
	f.reportID = runID
	return f.report, f.reportErr
}

type auditEntry struct {
	runID      string
	action     string
	actorKeyID string
	detail     string
}

func (f *fakeUseCases) LogAudit(ctx context.Context, runID, action, actorKeyID, detail string) error {
	f.auditEntries = append(f.auditEntries, auditEntry{
		runID:      runID,
		action:     action,
		actorKeyID: actorKeyID,
		detail:     detail,
	})
	return nil
}

func newAuthStoreWithKey(t *testing.T, scopes []string) (*auth.MemoryStore, string) {
	t.Helper()
	st, err := auth.NewMemoryStore()
	if err != nil {
		t.Fatalf("new memory store: %v", err)
	}
	generated, err := auth.Generate("test-key", scopes)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	if err := st.Upsert(generated.Record); err != nil {
		t.Fatalf("upsert key: %v", err)
	}
	return st, generated.Secret
}

func authHeader(secret string) string {
	return "Bearer " + secret
}

func TestListScenariosSuccess(t *testing.T) {
	uc := &fakeUseCases{
		scenarios: []app.ScenarioSummary{
			{Key: "alpha", Version: 1, Name: "Alpha", Description: "desc a"},
			{Key: "zeta", Version: 2, Name: "Zeta", Description: "desc z"},
		},
	}
	st, secret := newAuthStoreWithKey(t, []string{auth.ScopeRead})
	mux := http.NewServeMux()
	Register(mux, uc, st, sse.NewHub(10))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/scenarios", nil)
	req.Header.Set("Authorization", authHeader(secret))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	res := rec.Result()
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", res.StatusCode)
	}
	var body struct {
		Scenarios []app.ScenarioSummary `json:"scenarios"`
	}
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(body.Scenarios) != 2 {
		t.Fatalf("expected 2 scenarios, got %d", len(body.Scenarios))
	}
}

func TestListScenariosRequiresAuth(t *testing.T) {
	uc := &fakeUseCases{}
	st, err := auth.NewMemoryStore()
	if err != nil {
		t.Fatalf("new memory store: %v", err)
	}
	mux := http.NewServeMux()
	Register(mux, uc, st, sse.NewHub(10))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/scenarios", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 without auth, got %d", rec.Code)
	}
}

func TestCreateRunSuccess(t *testing.T) {
	createdRun := &store.Run{
		ID:           "run-1",
		ScenarioID:   "duplicate_delivery",
		Status:       string(store.RunStatePending),
		AttemptCount: 1,
		Version:      1,
	}
	uc := &fakeUseCases{createRun: createdRun, created: true}
	st, secret := newAuthStoreWithKey(t, []string{auth.ScopeRun})
	mux := http.NewServeMux()
	Register(mux, uc, st, sse.NewHub(10))

	body := strings.NewReader(`{"scenario_key":"duplicate_delivery"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/runs", body)
	req.Header.Set("Authorization", authHeader(secret))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	res := rec.Result()
	defer res.Body.Close()
	if res.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d", res.StatusCode)
	}
}

func TestCreateRunValidationError(t *testing.T) {
	uc := &fakeUseCases{}
	st, secret := newAuthStoreWithKey(t, []string{auth.ScopeRun})
	mux := http.NewServeMux()
	Register(mux, uc, st, sse.NewHub(10))

	body := strings.NewReader(`{}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/runs", body)
	req.Header.Set("Authorization", authHeader(secret))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing scenario_key, got %d", rec.Code)
	}
}

func TestCreateRunRequiresRunScope(t *testing.T) {
	uc := &fakeUseCases{}
	st, secret := newAuthStoreWithKey(t, []string{auth.ScopeRead})
	mux := http.NewServeMux()
	Register(mux, uc, st, sse.NewHub(10))

	body := strings.NewReader(`{"scenario_key":"duplicate_delivery"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/runs", body)
	req.Header.Set("Authorization", authHeader(secret))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for read-only key on create, got %d", rec.Code)
	}
}

func TestCreateRunUnknownScenario(t *testing.T) {
	uc := &fakeUseCases{createErr: orchestrator.ErrScenarioNotFound}
	st, secret := newAuthStoreWithKey(t, []string{auth.ScopeRun})
	mux := http.NewServeMux()
	Register(mux, uc, st, sse.NewHub(10))

	body := strings.NewReader(`{"scenario_key":"missing"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/runs", body)
	req.Header.Set("Authorization", authHeader(secret))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for unknown scenario, got %d", rec.Code)
	}
}

func TestCreateRunIdempotency(t *testing.T) {
	run := &store.Run{
		ID:           "run-1",
		ScenarioID:   "duplicate_delivery",
		Status:       string(store.RunStatePending),
		AttemptCount: 1,
		Version:      1,
	}
	uc := &fakeUseCases{createRun: run, created: true}
	st, secret := newAuthStoreWithKey(t, []string{auth.ScopeRun})
	mux := http.NewServeMux()
	Register(mux, uc, st, sse.NewHub(10))

	body := strings.NewReader(`{"scenario_key":"duplicate_delivery"}`)

	req1 := httptest.NewRequest(http.MethodPost, "/api/v1/runs", body)
	req1.Header.Set("Authorization", authHeader(secret))
	req1.Header.Set("Idempotency-Key", "key-1")
	req1.Header.Set("Content-Type", "application/json")
	rec1 := httptest.NewRecorder()
	mux.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusCreated {
		t.Fatalf("expected 201 on first request, got %d", rec1.Code)
	}

	// Second request with same idempotency key should return 200 (not a new run)
	uc.created = false
	req2 := httptest.NewRequest(http.MethodPost, "/api/v1/runs", strings.NewReader(`{"scenario_key":"duplicate_delivery"}`))
	req2.Header.Set("Authorization", authHeader(secret))
	req2.Header.Set("Idempotency-Key", "key-1")
	req2.Header.Set("Content-Type", "application/json")
	rec2 := httptest.NewRecorder()
	mux.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusOK {
		t.Fatalf("expected 200 on idempotent replay, got %d", rec2.Code)
	}
}

func TestListRunsSuccess(t *testing.T) {
	uc := &fakeUseCases{
		listRuns: []app.RunSummary{
			{ID: "run-1", ScenarioID: "scenario-a", Status: string(store.RunStatePending)},
			{ID: "run-2", ScenarioID: "scenario-a", Status: string(store.RunStateCompleted)},
		},
		listTotal: 2,
	}
	st, secret := newAuthStoreWithKey(t, []string{auth.ScopeRead})
	mux := http.NewServeMux()
	Register(mux, uc, st, sse.NewHub(10))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/runs?scenario_id=scenario-a&limit=10&offset=0", nil)
	req.Header.Set("Authorization", authHeader(secret))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var body struct {
		Runs   []app.RunSummary `json:"runs"`
		Total  int              `json:"total"`
		Limit  int              `json:"limit"`
		Offset int              `json:"offset"`
	}
	if err := json.NewDecoder(rec.Result().Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(body.Runs) != 2 || body.Total != 2 {
		t.Fatalf("expected 2 runs and total 2, got %d runs and total %d", len(body.Runs), body.Total)
	}
}

func TestListRunsRequiresAuth(t *testing.T) {
	uc := &fakeUseCases{}
	st, err := auth.NewMemoryStore()
	if err != nil {
		t.Fatalf("new memory store: %v", err)
	}
	mux := http.NewServeMux()
	Register(mux, uc, st, sse.NewHub(10))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/runs?scenario_id=x", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 without auth, got %d", rec.Code)
	}
}

func TestGetRunSuccess(t *testing.T) {
	uc := &fakeUseCases{
		getRun: &store.Run{
			ID:           "run-1",
			ScenarioID:   "scenario-a",
			Status:       string(store.RunStateCompleted),
			AttemptCount: 3,
			Version:      1,
		},
	}
	st, secret := newAuthStoreWithKey(t, []string{auth.ScopeRead})
	mux := http.NewServeMux()
	Register(mux, uc, st, sse.NewHub(10))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/runs/run-1", nil)
	req.Header.Set("Authorization", authHeader(secret))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var body struct {
		Run *store.Run `json:"run"`
	}
	if err := json.NewDecoder(rec.Result().Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Run.ID != "run-1" {
		t.Fatalf("expected run-1, got %q", body.Run.ID)
	}
}

func TestGetRunNotFound(t *testing.T) {
	uc := &fakeUseCases{getErr: pgx.ErrNoRows}
	st, secret := newAuthStoreWithKey(t, []string{auth.ScopeRead})
	mux := http.NewServeMux()
	Register(mux, uc, st, sse.NewHub(10))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/runs/no-such-run", nil)
	req.Header.Set("Authorization", authHeader(secret))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestCancelRunSuccess(t *testing.T) {
	cancelledRun := &store.Run{
		ID:                 "run-1",
		ScenarioID:         "scenario-a",
		Status:             string(store.RunStateCancelled),
		CancellationReason: strPtr("cancelled via API"),
		AttemptCount:       3,
		Version:            2,
	}
	uc := &fakeUseCases{cancelRun: cancelledRun}
	st, secret := newAuthStoreWithKey(t, []string{auth.ScopeRun})
	mux := http.NewServeMux()
	Register(mux, uc, st, sse.NewHub(10))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/runs/run-1/cancel", nil)
	req.Header.Set("Authorization", authHeader(secret))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var body struct {
		Run *store.Run `json:"run"`
	}
	if err := json.NewDecoder(rec.Result().Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Run.ID != "run-1" {
		t.Fatalf("expected run-1, got %q", body.Run.ID)
	}
	if body.Run.Status != string(store.RunStateCancelled) {
		t.Fatalf("expected cancelled status, got %q", body.Run.Status)
	}
	if uc.cancelID != "run-1" {
		t.Fatalf("expected cancelID run-1, got %q", uc.cancelID)
	}
}

func TestCancelRunRequiresAuth(t *testing.T) {
	uc := &fakeUseCases{}
	st, err := auth.NewMemoryStore()
	if err != nil {
		t.Fatalf("new memory store: %v", err)
	}
	mux := http.NewServeMux()
	Register(mux, uc, st, sse.NewHub(10))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/runs/run-1/cancel", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 without auth, got %d", rec.Code)
	}
}

func TestCancelRunRequiresRunScope(t *testing.T) {
	uc := &fakeUseCases{}
	st, secret := newAuthStoreWithKey(t, []string{auth.ScopeRead})
	mux := http.NewServeMux()
	Register(mux, uc, st, sse.NewHub(10))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/runs/run-1/cancel", nil)
	req.Header.Set("Authorization", authHeader(secret))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for read-only key on cancel, got %d", rec.Code)
	}
}

func TestCancelRunNotFound(t *testing.T) {
	uc := &fakeUseCases{cancelErr: pgx.ErrNoRows}
	st, secret := newAuthStoreWithKey(t, []string{auth.ScopeRun})
	mux := http.NewServeMux()
	Register(mux, uc, st, sse.NewHub(10))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/runs/no-such-run/cancel", nil)
	req.Header.Set("Authorization", authHeader(secret))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestCancelRunAlreadyTerminal(t *testing.T) {
	uc := &fakeUseCases{cancelErr: store.ErrInvalidTransition}
	st, secret := newAuthStoreWithKey(t, []string{auth.ScopeRun})
	mux := http.NewServeMux()
	Register(mux, uc, st, sse.NewHub(10))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/runs/run-1/cancel", nil)
	req.Header.Set("Authorization", authHeader(secret))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d", rec.Code)
	}
}

func TestGetReportSuccess(t *testing.T) {
	rep := &report.Report{
		SchemaVersion: report.SchemaVersion,
		Run: report.RunSummary{
			ID:           "run-1",
			ScenarioID:   "scenario-a",
			Status:       string(store.RunStateCompleted),
			AttemptCount: 3,
			Version:      1,
		},
	}
	uc := &fakeUseCases{report: rep}
	st, secret := newAuthStoreWithKey(t, []string{auth.ScopeRead})
	mux := http.NewServeMux()
	Register(mux, uc, st, sse.NewHub(10))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/runs/run-1/report", nil)
	req.Header.Set("Authorization", authHeader(secret))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Fatalf("expected application/json content type, got %q", ct)
	}
	if uc.reportID != "run-1" {
		t.Fatalf("expected reportID run-1, got %q", uc.reportID)
	}
	var body map[string]any
	if err := json.NewDecoder(rec.Result().Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body["schema_version"] != report.SchemaVersion {
		t.Fatalf("expected schema_version %q, got %v", report.SchemaVersion, body["schema_version"])
	}
}

func TestGetReportRequiresAuth(t *testing.T) {
	uc := &fakeUseCases{}
	st, err := auth.NewMemoryStore()
	if err != nil {
		t.Fatalf("new memory store: %v", err)
	}
	mux := http.NewServeMux()
	Register(mux, uc, st, sse.NewHub(10))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/runs/run-1/report", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 without auth, got %d", rec.Code)
	}
}

func TestGetReportNotFound(t *testing.T) {
	uc := &fakeUseCases{reportErr: pgx.ErrNoRows}
	st, secret := newAuthStoreWithKey(t, []string{auth.ScopeRead})
	mux := http.NewServeMux()
	Register(mux, uc, st, sse.NewHub(10))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/runs/no-such-run/report", nil)
	req.Header.Set("Authorization", authHeader(secret))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestExportReportJSON(t *testing.T) {
	rep := &report.Report{
		SchemaVersion: report.SchemaVersion,
		Run: report.RunSummary{
			ID:           "run-1",
			ScenarioID:   "scenario-a",
			Status:       string(store.RunStateCompleted),
			AttemptCount: 3,
			Version:      1,
		},
	}
	uc := &fakeUseCases{report: rep}
	st, secret := newAuthStoreWithKey(t, []string{auth.ScopeRead})
	mux := http.NewServeMux()
	Register(mux, uc, st, sse.NewHub(10))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/runs/run-1/export", nil)
	req.Header.Set("Authorization", authHeader(secret))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Fatalf("expected application/json content type, got %q", ct)
	}
	if cd := rec.Header().Get("Content-Disposition"); !strings.Contains(cd, "attachment") {
		t.Fatalf("expected attachment disposition, got %q", cd)
	}
}

func TestExportReportCSV(t *testing.T) {
	rep := &report.Report{
		SchemaVersion: report.SchemaVersion,
		Run: report.RunSummary{
			ID:           "run-1",
			ScenarioID:   "scenario-a",
			Status:       string(store.RunStateCompleted),
			AttemptCount: 3,
			Version:      1,
		},
		Timestamps: report.Timestamps{
			CreatedAt: time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC),
			UpdatedAt: time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC),
		},
		InvariantResults: []report.InvariantResultEntry{
			{InvariantKey: "no_duplicate_payment", Verdict: string(invariants.StatusPass)},
			{InvariantKey: "no_negative_inventory", Verdict: string(invariants.StatusFail)},
		},
	}
	uc := &fakeUseCases{report: rep}
	st, secret := newAuthStoreWithKey(t, []string{auth.ScopeRead})
	mux := http.NewServeMux()
	Register(mux, uc, st, sse.NewHub(10))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/runs/run-1/export.csv", nil)
	req.Header.Set("Authorization", authHeader(secret))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "text/csv") {
		t.Fatalf("expected text/csv content type, got %q", ct)
	}
	body := rec.Result().Body
	defer body.Close()
	data, err := io.ReadAll(body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	csv := string(data)
	if !strings.Contains(csv, "run_id,scenario_id,status") {
		t.Fatalf("expected CSV header, got: %s", csv)
	}
	if !strings.Contains(csv, "no_duplicate_payment") {
		t.Fatalf("expected invariant key in CSV, got: %s", csv)
	}
	if !strings.Contains(csv, "pass") {
		t.Fatalf("expected pass verdict in CSV, got: %s", csv)
	}
}

func strPtr(s string) *string { return &s }
