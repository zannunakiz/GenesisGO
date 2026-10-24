// Package httpapi provides HTTP handlers and routing for GenesisGo.
//
// Routes added here are mounted on the same mux as health endpoints and share
// the middleware chain (request ID, logging, recovery, timeout). Control-plane
// routes are individually protected by RequireAuth.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/zannunakiz/GenesisGO/internal/app"
	"github.com/zannunakiz/GenesisGO/internal/auth"
	"github.com/zannunakiz/GenesisGO/internal/orchestrator"
	"github.com/zannunakiz/GenesisGO/internal/report"
	"github.com/zannunakiz/GenesisGO/internal/sse"
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
	CancelRun(ctx context.Context, id string) (*store.Run, error)
	GetReport(ctx context.Context, runID string) (*report.Report, error)
}

// Register wires run and scenario endpoints onto mux. Control-plane routes are
// protected by RequireAuth; health endpoints remain unauthenticated.
func Register(mux *http.ServeMux, uc useCases, authStore auth.Store, hub *sse.Hub) {
	mux.Handle("GET /api/v1/scenarios", RequireAuth(authStore, auth.ScopeRead)(http.HandlerFunc(handleListScenarios(uc))))
	mux.Handle("POST /api/v1/runs", RequireAuth(authStore, auth.ScopeRun)(http.HandlerFunc(handleCreateRun(uc))))
	mux.Handle("GET /api/v1/runs", RequireAuth(authStore, auth.ScopeRead)(http.HandlerFunc(handleListRuns(uc))))
	mux.Handle("GET /api/v1/runs/{id}", RequireAuth(authStore, auth.ScopeRead)(http.HandlerFunc(handleGetRun(uc))))
	mux.Handle("POST /api/v1/runs/{id}/cancel", RequireAuth(authStore, auth.ScopeRun)(http.HandlerFunc(handleCancelRun(uc))))
	mux.Handle("GET /api/v1/runs/{id}/report", RequireAuth(authStore, auth.ScopeRead)(http.HandlerFunc(handleGetReport(uc))))
	mux.Handle("GET /api/v1/runs/{id}/export", RequireAuth(authStore, auth.ScopeRead)(http.HandlerFunc(handleExportReport(uc))))
	mux.Handle("GET /api/v1/runs/{id}/export.csv", RequireAuth(authStore, auth.ScopeRead)(http.HandlerFunc(handleExportReportCSV(uc))))
	mux.Handle("GET /api/v1/runs/{id}/events", RequireAuth(authStore, auth.ScopeRead)(http.HandlerFunc(handleSSEEvents(hub))))
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

// handleSSEEvents streams run-scoped lifecycle events via Server-Sent Events.
// It authenticates the caller, subscribes to the run's event hub, and writes
// events as text/event-stream until the client disconnects or the context is
// cancelled. Heartbeats keep idle connections alive.
func handleSSEEvents(hub *sse.Hub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimSpace(r.PathValue("id"))
		if id == "" {
			NotFound(w, r, "run not found")
			return
		}

		// SSE requires a persistent connection; disable buffering.
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("X-Accel-Buffering", "no")

		// Support Last-Event-ID for reconnection replay.
		lastEventID := strings.TrimSpace(r.Header.Get("Last-Event-ID"))

		ch := hub.Subscribe(id, lastEventID)
		defer hub.Unsubscribe(ch)

		ctx := r.Context()
		for {
			select {
			case <-ctx.Done():
				return
			case ev, ok := <-ch:
				if !ok {
					return
				}
				payload, err := json.Marshal(ev)
				if err != nil {
					continue
				}
				_, _ = fmt.Fprintf(w, "id: %s\ndata: %s\n\n", ev.ID, payload)
				if f, ok := w.(http.Flusher); ok {
					f.Flush()
				}
			}
		}
	}
}

// handleCancelRun cancels a queued or running run. It transitions the run to
// cancelled and signals downstream work to stop via context cancellation.
func handleCancelRun(uc useCases) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimSpace(r.PathValue("id"))
		if id == "" {
			NotFound(w, r, "run not found")
			return
		}

		run, err := uc.CancelRun(r.Context(), id)
		if err != nil {
			switch {
			case errors.Is(err, pgx.ErrNoRows):
				NotFound(w, r, "run not found")
			case errors.Is(err, orchestrator.ErrRunNotFound):
				NotFound(w, r, "run not found")
			case errors.Is(err, store.ErrInvalidTransition):
				Conflict(w, r, "run is already terminal or cannot be cancelled")
			default:
				Internal(w, r, err)
			}
			return
		}
		WriteJSON(w, r, http.StatusOK, map[string]any{"run": run})
	}
}

// handleGetReport returns a reproducible run report as JSON.
func handleGetReport(uc useCases) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimSpace(r.PathValue("id"))
		if id == "" {
			NotFound(w, r, "run not found")
			return
		}

		rep, err := uc.GetReport(r.Context(), id)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) || errors.Is(err, store.ErrRunNotFound) {
				NotFound(w, r, "report not found")
				return
			}
			Internal(w, r, err)
			return
		}

		payload, err := rep.JSON()
		if err != nil {
			Internal(w, r, err)
			return
		}

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("X-Request-ID", requestIDFor(r))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(payload)
	}
}

// handleExportReport exports a run report as a downloadable JSON file.
func handleExportReport(uc useCases) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimSpace(r.PathValue("id"))
		if id == "" {
			NotFound(w, r, "run not found")
			return
		}

		rep, err := uc.GetReport(r.Context(), id)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) || errors.Is(err, store.ErrRunNotFound) {
				NotFound(w, r, "report not found")
				return
			}
			Internal(w, r, err)
			return
		}

		payload, err := rep.JSON()
		if err != nil {
			Internal(w, r, err)
			return
		}

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"run-%s-report.json\"", id))
		w.Header().Set("X-Request-ID", requestIDFor(r))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(payload)
	}
}

// handleExportReportCSV exports a CSV summary of run and invariant results.
func handleExportReportCSV(uc useCases) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimSpace(r.PathValue("id"))
		if id == "" {
			NotFound(w, r, "run not found")
			return
		}

		rep, err := uc.GetReport(r.Context(), id)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) || errors.Is(err, store.ErrRunNotFound) {
				NotFound(w, r, "report not found")
				return
			}
			Internal(w, r, err)
			return
		}

		var buf strings.Builder
		buf.WriteString("run_id,scenario_id,status,attempt_count,version,created_at,started_at,completed_at\n")
		buf.WriteString(fmt.Sprintf("%s,%s,%s,%d,%d,%s,%s,%s\n",
			rep.Run.ID,
			rep.Run.ScenarioID,
			rep.Run.Status,
			rep.Run.AttemptCount,
			rep.Run.Version,
			rep.Timestamps.CreatedAt.Format(time.RFC3339),
			timestampPtr(rep.Timestamps.StartedAt),
			timestampPtr(rep.Timestamps.CompletedAt),
		))
		buf.WriteString("\n")
		buf.WriteString("invariant_key,verdict\n")
		for _, inv := range rep.InvariantResults {
			buf.WriteString(fmt.Sprintf("%s,%s\n", inv.InvariantKey, inv.Verdict))
		}

		w.Header().Set("Content-Type", "text/csv; charset=utf-8")
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"run-%s-report.csv\"", id))
		w.Header().Set("X-Request-ID", requestIDFor(r))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(buf.String()))
	}
}

func timestampPtr(t *time.Time) string {
	if t == nil {
		return ""
	}
	return t.Format(time.RFC3339)
}
