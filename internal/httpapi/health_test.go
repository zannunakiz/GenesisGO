package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func decodeHealth(t *testing.T, body []byte) HealthResponse {
	t.Helper()
	var payload HealthResponse
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("health response must be JSON: %v", err)
	}
	return payload
}

func TestHealthEndpointIsStable(t *testing.T) {
	start := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	deps := HealthDeps{
		StartTime:          start,
		Now:                func() time.Time { return start.Add(42 * time.Second) },
		DatabaseConfigured: true,
		NATSConfigured:     true,
		ArcjetEnabled:      false,
		AIConfigured:       false,
	}
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	HealthHandler(deps).ServeHTTP(rec, req)

	res := rec.Result()
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", res.StatusCode)
	}
	if ctype := res.Header.Get("Content-Type"); !strings.Contains(ctype, "application/json") {
		t.Fatalf("expected JSON content type, got %q", ctype)
	}
	if res.Header.Get("X-Request-ID") == "" {
		t.Fatal("health response must carry correlation ID")
	}
	payload := decodeHealth(t, readAll(t, res))
	if payload.Status != "ok" || payload.UptimeSecs != 42 {
		t.Fatalf("unexpected health payload: %+v", payload)
	}
	if payload.Dependencies.Database != "configured" || payload.Dependencies.NATS != "configured" {
		t.Fatalf("unexpected dependency labels: %+v", payload.Dependencies)
	}
	if payload.Dependencies.Arcjet != "disabled" || payload.Dependencies.AI != "not_configured" {
		t.Fatalf("unexpected optional labels: %+v", payload.Dependencies)
	}
}

func TestHealthEndpointWorksWithoutOptionalServices(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	HealthHandler(HealthDeps{}).ServeHTTP(rec, req)
	res := rec.Result()
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", res.StatusCode)
	}
	payload := decodeHealth(t, readAll(t, res))
	if payload.Status != "ok" {
		t.Fatalf("unexpected payload: %+v", payload)
	}
}

func TestHealthEndpointRejectsUnexpectedMethod(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/healthz", nil)
	rec := httptest.NewRecorder()
	HealthHandler(HealthDeps{}).ServeHTTP(rec, req)
	res := rec.Result()
	defer res.Body.Close()
	if res.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", res.StatusCode)
	}
	if envelope := decodeEnvelope(t, res.Body); envelope.Code != CodeMethodNotAllowed {
		t.Fatalf("unexpected envelope: %+v", envelope)
	}
}

func TestHealthMuxRegistersExpectedRoute(t *testing.T) {
	mux := NewMux(LiveHandler(LiveDeps{}), ReadyHandler(ReadyDeps{DatabaseConfigured: true, NATSConfigured: true}))
	for _, path := range []string{"/health/live", "/health/ready", "/healthz"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		res := rec.Result()
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("expected mux health route %s, got %d", path, res.StatusCode)
		}
	}
}

func TestLiveEndpointIgnoresMissingDependencies(t *testing.T) {
	start := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	req := httptest.NewRequest(http.MethodGet, "/health/live", nil)
	rec := httptest.NewRecorder()
	LiveHandler(LiveDeps{StartTime: start, Now: fixedTime(start, 7*time.Second)}).ServeHTTP(rec, req)

	res := rec.Result()
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", res.StatusCode)
	}
	var payload LiveResponse
	if err := json.Unmarshal(readAll(t, res), &payload); err != nil {
		t.Fatalf("live response must be JSON: %v", err)
	}
	if payload.Status != "ok" || payload.UptimeSecs != 7 {
		t.Fatalf("unexpected live payload: %+v", payload)
	}
}

func TestReadyEndpointReportsReadyAndNotReady(t *testing.T) {
	start := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	ready := ReadyDeps{
		StartTime:          start,
		Now:                fixedTime(start, 9*time.Second),
		DatabaseConfigured: true,
		NATSConfigured:     true,
	}
	req := httptest.NewRequest(http.MethodGet, "/health/ready", nil)
	rec := httptest.NewRecorder()
	ReadyHandler(ready).ServeHTTP(rec, req)
	res := rec.Result()
	payload := decodeReady(t, readAll(t, res))
	res.Body.Close()
	if res.StatusCode != http.StatusOK || payload.Status != "ready" || len(payload.Missing) != 0 {
		t.Fatalf("unexpected ready payload: status=%d body=%+v", res.StatusCode, payload)
	}

	notReady := ReadyDeps{StartTime: start, Now: fixedTime(start, 9*time.Second)}
	req = httptest.NewRequest(http.MethodGet, "/health/ready", nil)
	rec = httptest.NewRecorder()
	ReadyHandler(notReady).ServeHTTP(rec, req)
	res = rec.Result()
	payload = decodeReady(t, readAll(t, res))
	res.Body.Close()
	if res.StatusCode != http.StatusServiceUnavailable || payload.Status != "not_ready" {
		t.Fatalf("expected not-ready verdict, got status=%d body=%+v", res.StatusCode, payload)
	}
	if len(payload.Missing) != 2 || payload.Missing[0] != "database" || payload.Missing[1] != "nats" {
		t.Fatalf("expected missing dependencies, got %+v", payload)
	}
}

func TestReadyEndpointIgnoresOptionalIntegrations(t *testing.T) {
	deps := ReadyDeps{DatabaseConfigured: true, NATSConfigured: true, ArcjetEnabled: true, AIConfigured: false}
	req := httptest.NewRequest(http.MethodGet, "/health/ready", nil)
	rec := httptest.NewRecorder()
	ReadyHandler(deps).ServeHTTP(rec, req)
	res := rec.Result()
	payload := decodeReady(t, readAll(t, res))
	res.Body.Close()
	if res.StatusCode != http.StatusOK || payload.Status != "ready" {
		t.Fatalf("optional integrations must not gate readiness: status=%d body=%+v", res.StatusCode, payload)
	}
	if payload.Dependencies.Arcjet != "enabled" || payload.Dependencies.AI != "not_configured" {
		t.Fatalf("optional labels must remain informational: %+v", payload.Dependencies)
	}
}

func TestHealthEndpointsRejectUnexpectedMethods(t *testing.T) {
	handlers := map[string]http.Handler{
		"/health/live":  LiveHandler(LiveDeps{}),
		"/health/ready": ReadyHandler(ReadyDeps{}),
	}
	for path, handler := range handlers {
		req := httptest.NewRequest(http.MethodPost, path, nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		res := rec.Result()
		envelope := decodeEnvelope(t, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusMethodNotAllowed || envelope.Code != CodeMethodNotAllowed {
			t.Fatalf("expected 405 envelope for %s, got %d %+v", path, res.StatusCode, envelope)
		}
	}
}

func decodeReady(t *testing.T, body []byte) ReadyResponse {
	t.Helper()
	var payload ReadyResponse
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("ready response must be JSON: %v", err)
	}
	return payload
}
