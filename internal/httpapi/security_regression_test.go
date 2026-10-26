package httpapi

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/zannunakiz/GenesisGO/internal/auth"
	"github.com/zannunakiz/GenesisGO/internal/protection"
	"github.com/zannunakiz/GenesisGO/internal/sse"
)

// TestSpoofedForwardingHeaderIgnoredFromUntrustedPeer ensures that an
// X-Forwarded-For header supplied by a direct peer that is not a trusted
// proxy cannot spoof the client IP seen by the fallback limiter.
func TestSpoofedForwardingHeaderIgnoredFromUntrustedPeer(t *testing.T) {
	guard, err := protection.New(protection.Settings{
		Enabled:           false,
		FallbackPerMinute: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	first := httptest.NewRequest(http.MethodPost, "/v1/runs", nil)
	first.RemoteAddr = "203.0.113.40:4000"
	rec := httptest.NewRecorder()
	guardedRoute(guard).ServeHTTP(rec, first)
	rec.Result().Body.Close()

	second := httptest.NewRequest(http.MethodPost, "/v1/runs", nil)
	second.RemoteAddr = "203.0.113.40:4000"
	second.Header.Set("X-Forwarded-For", "1.2.3.4")
	rec = httptest.NewRecorder()
	guardedRoute(guard).ServeHTTP(rec, second)
	res := rec.Result()
	defer res.Body.Close()
	if res.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("expected 429 when spoofed XFF is ignored, got %d", res.StatusCode)
	}

	third := httptest.NewRequest(http.MethodPost, "/v1/runs", nil)
	third.RemoteAddr = "203.0.113.41:4000"
	rec = httptest.NewRecorder()
	guardedRoute(guard).ServeHTTP(rec, third)
	res = rec.Result()
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 for a different peer, got %d", res.StatusCode)
	}
}

// TestPathGuessingDoesNotLeakServerErrors ensures that IDs resembling path
// traversal or other guessable patterns are handled safely and never produce
// 500 responses from the handler layer.
func TestPathGuessingDoesNotLeakServerErrors(t *testing.T) {
	uc := &fakeUseCases{}
	st, secret := newAuthStoreWithKey(t, []string{auth.ScopeRead})
	mux := http.NewServeMux()
	Register(mux, uc, st, sse.NewHub(10))

	guessableIDs := []string{
		"../run-1",
		"../../etc/passwd",
		"run-1/../run-2",
		"%2e%2e%2frun-1",
		"run-1%00",
		"../../../run-1",
	}
	for _, id := range guessableIDs {
		t.Run(id, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/api/v1/runs/"+id, nil)
			req.Header.Set("Authorization", authHeader(secret))
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)
			res := rec.Result()
			defer res.Body.Close()
			if res.StatusCode == http.StatusInternalServerError {
				t.Fatalf("ID %q produced a 500; expected 404 or 400, got %d", id, res.StatusCode)
			}
		})
	}
}

// TestCrossRunDataAccess ensures handlers operate on the run ID taken from
// the request path and do not leak data from a different run.
func TestCrossRunDataAccess(t *testing.T) {
	uc := &fakeUseCases{}
	st, secret := newAuthStoreWithKey(t, []string{auth.ScopeRead})
	mux := http.NewServeMux()
	Register(mux, uc, st, sse.NewHub(10))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/runs/run-a", nil)
	req.Header.Set("Authorization", authHeader(secret))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	res := rec.Result()
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 for run-a, got %d", res.StatusCode)
	}
	if uc.getRunID != "run-a" {
		t.Fatalf("expected GetRun called with run-a, got %q", uc.getRunID)
	}

	uc.getRunID = ""
	req = httptest.NewRequest(http.MethodGet, "/api/v1/runs/run-b", nil)
	req.Header.Set("Authorization", authHeader(secret))
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	res = rec.Result()
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 for run-b, got %d", res.StatusCode)
	}
	if uc.getRunID != "run-b" {
		t.Fatalf("expected GetRun called with run-b, got %q", uc.getRunID)
	}

	uc.reportID = ""
	req = httptest.NewRequest(http.MethodGet, "/api/v1/runs/run-a/report", nil)
	req.Header.Set("Authorization", authHeader(secret))
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	res = rec.Result()
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 for run-a report, got %d", res.StatusCode)
	}
	if uc.reportID != "run-a" {
		t.Fatalf("expected GetReport called with run-a, got %q", uc.reportID)
	}
}
