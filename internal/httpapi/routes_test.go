package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/zannunakiz/GenesisGO/internal/app"
	"github.com/zannunakiz/GenesisGO/internal/auth"
	"github.com/zannunakiz/GenesisGO/internal/orchestrator"
	"github.com/zannunakiz/GenesisGO/internal/store"
)

// fakeUseCases implements app use cases with controlled behavior for tests.
type fakeUseCases struct {
	scenarios []app.ScenarioSummary
	scenErr   error
	createRun *store.Run
	createErr error
	created   bool
	listRuns  []app.RunSummary
	listTotal int
	listErr   error
	getRun    *store.Run
	getErr    error
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
	Register(mux, uc, st)

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
	Register(mux, uc, st)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/scenarios", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 without auth, got %d", rec.Code)
	}
}

func TestCreateRunSuccess(t *testing.T) {
	createdRun := &store.Run{
		ID:          "run-1",
		ScenarioID:  "duplicate_delivery",
		Status:      string(store.RunStatePending),
		AttemptCount: 1,
		Version:     1,
	}
	uc := &fakeUseCases{createRun: createdRun, created: true}
	st, secret := newAuthStoreWithKey(t, []string{auth.ScopeRun})
	mux := http.NewServeMux()
	Register(mux, uc, st)

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
	Register(mux, uc, st)

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
	Register(mux, uc, st)

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
	Register(mux, uc, st)

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
		ID:          "run-1",
		ScenarioID:  "duplicate_delivery",
		Status:      string(store.RunStatePending),
		AttemptCount: 1,
		Version:     1,
	}
	uc := &fakeUseCases{createRun: run, created: true}
	st, secret := newAuthStoreWithKey(t, []string{auth.ScopeRun})
	mux := http.NewServeMux()
	Register(mux, uc, st)

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
	Register(mux, uc, st)

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
	Register(mux, uc, st)

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
			ID:          "run-1",
			ScenarioID:  "scenario-a",
			Status:      string(store.RunStateCompleted),
			AttemptCount: 3,
			Version:     1,
		},
	}
	st, secret := newAuthStoreWithKey(t, []string{auth.ScopeRead})
	mux := http.NewServeMux()
	Register(mux, uc, st)

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
	Register(mux, uc, st)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/runs/no-such-run", nil)
	req.Header.Set("Authorization", authHeader(secret))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

