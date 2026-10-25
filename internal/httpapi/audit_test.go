package httpapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/zannunakiz/GenesisGO/internal/auth"
	"github.com/zannunakiz/GenesisGO/internal/report"
	"github.com/zannunakiz/GenesisGO/internal/sse"
	"github.com/zannunakiz/GenesisGO/internal/store"
)

func TestCreateRunRecordsAuditEntry(t *testing.T) {
	uc := &fakeUseCases{
		createRun: &store.Run{ID: "run-1", ScenarioID: "scenario-a", Status: string(store.RunStatePending)},
		created:   true,
	}
	st, secret := newAuthStoreWithKey(t, []string{auth.ScopeRun})
	mux := http.NewServeMux()
	Register(mux, uc, st, sse.NewHub(10))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/runs", strings.NewReader(`{"scenario_key":"scenario-a"}`))
	req.Header.Set("Authorization", authHeader(secret))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", rec.Code)
	}
	if len(uc.auditEntries) != 1 {
		t.Fatalf("expected 1 audit entry, got %d", len(uc.auditEntries))
	}
	e := uc.auditEntries[0]
	if e.action != "run.created" || e.actorKeyID != "test-key" || e.runID != "run-1" {
		t.Fatalf("unexpected audit entry: %+v", e)
	}
	if !strings.Contains(e.detail, `scenario_key="scenario-a"`) {
		t.Fatalf("expected scenario key in detail, got %q", e.detail)
	}
}

func TestCancelRunRecordsAuditEntry(t *testing.T) {
	uc := &fakeUseCases{cancelRun: &store.Run{ID: "run-1", Status: string(store.RunStateCancelled)}}
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
	if len(uc.auditEntries) != 1 || uc.auditEntries[0].action != "run.canceled" || uc.auditEntries[0].actorKeyID != "test-key" {
		t.Fatalf("unexpected audit entries: %+v", uc.auditEntries)
	}
}

func TestReportAccessRecordsAuditEntry(t *testing.T) {
	uc := &fakeUseCases{report: &report.Report{}}
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
	var actions []string
	for _, e := range uc.auditEntries {
		actions = append(actions, e.action)
	}
	if len(actions) != 1 || actions[0] != "report.accessed" {
		t.Fatalf("expected report.accessed audit, got %v", actions)
	}
}

var _ = context.Background
