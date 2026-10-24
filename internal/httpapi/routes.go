// Package httpapi provides HTTP handlers and routing for GenesisGo.
//
// Routes added here are mounted on the same mux as health endpoints and share
// the middleware chain (request ID, logging, recovery, timeout). Control-plane
// routes are individually protected by RequireAuth.
package httpapi

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/zannunakiz/GenesisGO/internal/app"
	"github.com/zannunakiz/GenesisGO/internal/auth"
	"github.com/zannunakiz/GenesisGO/internal/orchestrator"
	"github.com/zannunakiz/GenesisGO/internal/store"
)

// Pagination bounds.
const (
	defaultPageSize = 20
	maxPageSize     = 100
)

// useCases is the subset of app.UseCases needed by the HTTP handlers.
// Keeping it an interface lets tests inject fakes without a real database.
type useCases interface {
	ListScenarios(ctx context.Context) ([]app.ScenarioSummary, error)
	CreateRun(ctx context.Context, in app.CreateRunInput) (*store.Run, bool, error)
	ListRuns(ctx context.Context, in app.ListRunsInput) ([]app.RunSummary, int, error)
	GetRun(ctx context.Context, id string) (*store.Run, error)
}

// Register wires run and scenario endpoints onto mux. Control-plane routes are
// protected by RequireAuth; health endpoints remain unauthenticated.
func Register(mux *http.ServeMux, uc useCases, authStore auth.Store) {
	mux.Handle("GET /api/v1/scenarios", RequireAuth(authStore, auth.ScopeRead)(http.HandlerFunc(handleListScenarios(uc))))
	mux.Handle("POST /api/v1/runs", RequireAuth(authStore, auth.ScopeRun)(http.HandlerFunc(handleCreateRun(uc))))
	mux.Handle("GET /api/v1/runs", RequireAuth(authStore, auth.ScopeRead)(http.HandlerFunc(handleListRuns(uc))))
	mux.Handle("GET /api/v1/runs/{id}", RequireAuth(authStore, auth.ScopeRead)(http.HandlerFunc(handleGetRun(uc))))
}

// handleListScenarios lists built-in scenarios.
func handleListScenarios(uc useCases) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		list, err := uc.ListScenarios(r.Context())
		if err != nil {
			Internal(w, r, err)
			return
		}
		WriteJSON(w, r, http.StatusOK, map[string]any{"scenarios": list})
	}
}

// createRunRequest is the request body for creating a run.
type createRunRequest struct {
	ScenarioKey string `json:"scenario_key"`
}

// handleCreateRun creates a run for a scenario. Idempotency is provided via
// the Idempotency-Key request header.
func handleCreateRun(uc useCases) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req createRunRequest
		if !DecodeJSON(w, r, Limits{MaxBodyBytes: 1 << 10}, &req) {
			return
		}
		if strings.TrimSpace(req.ScenarioKey) == "" {
			BadRequest(w, r, CodeInvalidField, "scenario_key is required", []FieldError{{Field: "scenario_key", Reason: "is required"}})
			return
		}

		run, created, err := uc.CreateRun(r.Context(), app.CreateRunInput{
			ScenarioKey:    req.ScenarioKey,
			IdempotencyKey: strings.TrimSpace(r.Header.Get("Idempotency-Key")),
		})
		if err != nil {
			switch {
			case errors.Is(err, orchestrator.ErrScenarioNotFound):
				NotFound(w, r, "scenario not found")
			default:
				Internal(w, r, err)
			}
			return
		}

		status := http.StatusCreated
		if !created {
			status = http.StatusOK
		}
		WriteJSON(w, r, status, map[string]any{"run": run})
	}
}

// handleListRuns lists runs with optional scenario filter and pagination.
func handleListRuns(uc useCases) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		scenarioID := strings.TrimSpace(r.URL.Query().Get("scenario_id"))

		limit := defaultPageSize
		if v := strings.TrimSpace(r.URL.Query().Get("limit")); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil || n < 0 {
				BadRequest(w, r, CodeInvalidField, "limit must be a non-negative integer", []FieldError{{Field: "limit", Reason: "must be a non-negative integer"}})
				return
			}
			if n > maxPageSize {
				n = maxPageSize
			}
			if n == 0 {
				n = defaultPageSize
			}
			limit = n
		}

		offset := 0
		if v := strings.TrimSpace(r.URL.Query().Get("offset")); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil || n < 0 {
				BadRequest(w, r, CodeInvalidField, "offset must be a non-negative integer", []FieldError{{Field: "offset", Reason: "must be a non-negative integer"}})
				return
			}
			offset = n
		}

		list, total, err := uc.ListRuns(r.Context(), app.ListRunsInput{
			ScenarioID: scenarioID,
			Limit:      limit,
			Offset:     offset,
		})
		if err != nil {
			Internal(w, r, err)
			return
		}

		WriteJSON(w, r, http.StatusOK, map[string]any{
			"runs":   list,
			"total":  total,
			"limit":  limit,
			"offset": offset,
		})
	}
}

// handleGetRun returns a single run by ID.
func handleGetRun(uc useCases) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimSpace(r.PathValue("id"))
		if id == "" {
			NotFound(w, r, "run not found")
			return
		}

		run, err := uc.GetRun(r.Context(), id)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				NotFound(w, r, "run not found")
				return
			}
			Internal(w, r, err)
			return
		}
		WriteJSON(w, r, http.StatusOK, map[string]any{"run": run})
	}
}
