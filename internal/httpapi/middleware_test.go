package httpapi

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestValidatorBoundsStringsEnumsAndLists(t *testing.T) {
	var v Validator
	v.CheckString("name", "", 8, true)
	v.CheckString("nickname", strings.Repeat("x", 9), 8, false)
	v.CheckEnum("status", "weird", "queued", "running")
	v.CheckListLength("tags", 5, 2)
	if !v.Invalid() || len(v.Fields) != 4 {
		t.Fatalf("expected four field errors, got %+v", v.Fields)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/runs", nil)
	rec := httptest.NewRecorder()
	v.Fail(rec, req)
	res := rec.Result()
	defer res.Body.Close()
	envelope := decodeEnvelope(t, res.Body)
	if envelope.Code != CodeInvalidField || len(envelope.Fields) != 4 {
		t.Fatalf("unexpected envelope: %+v", envelope)
	}
}

func TestErrorShapesDoNotLeakDetails(t *testing.T) {
	secret := "super-secret-value"
	cases := []struct {
		name   string
		call   func(w http.ResponseWriter, r *http.Request)
		status int
		code   string
	}{
		{"not found", func(w http.ResponseWriter, r *http.Request) { NotFound(w, r, "") }, http.StatusNotFound, CodeNotFound},
		{"conflict", func(w http.ResponseWriter, r *http.Request) { Conflict(w, r, "") }, http.StatusConflict, CodeConflict},
		{"method", func(w http.ResponseWriter, r *http.Request) { MethodNotAllowed(w, r, "") }, http.StatusMethodNotAllowed, CodeMethodNotAllowed},
		{"internal", func(w http.ResponseWriter, r *http.Request) { Internal(w, r, errors.New(secret)) }, http.StatusInternalServerError, CodeInternal},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/missing", nil)
			rec := httptest.NewRecorder()
			tt.call(rec, req)
			res := rec.Result()
			defer res.Body.Close()
			if res.StatusCode != tt.status {
				t.Fatalf("expected %d, got %d", tt.status, res.StatusCode)
			}
			raw, err := io.ReadAll(res.Body)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(raw), secret) {
				t.Fatalf("error response leaked details: %s", raw)
			}
			var envelope ErrorEnvelope
			if err := jsonUnmarshal(raw, &envelope); err != nil {
				t.Fatal(err)
			}
			if envelope.Code != tt.code || envelope.RequestID == "" {
				t.Fatalf("unexpected envelope: %+v", envelope)
			}
		})
	}
}

func TestTimeoutMiddlewareReturnsSharedEnvelope(t *testing.T) {
	slow := Timeout(20 * time.Millisecond)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
			return
		case <-time.After(200 * time.Millisecond):
			WriteJSON(w, r, http.StatusOK, map[string]string{"ok": "true"})
		}
	}))
	req := httptest.NewRequest(http.MethodGet, "/v1/runs", nil)
	rec := httptest.NewRecorder()
	slow.ServeHTTP(rec, req)
	res := rec.Result()
	defer res.Body.Close()
	if res.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", res.StatusCode)
	}
	if envelope := decodeEnvelope(t, res.Body); envelope.Code != CodeRequestTimeout {
		t.Fatalf("unexpected envelope: %+v", envelope)
	}
}

func TestRecoverMiddlewareHidesPanicDetails(t *testing.T) {
	secret := "panic: sql connection string postgres://secret"
	handler := Recover(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { panic(secret) }))
	req := httptest.NewRequest(http.MethodGet, "/v1/runs", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	res := rec.Result()
	defer res.Body.Close()
	if res.StatusCode != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", res.StatusCode)
	}
	raw, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), secret) || strings.Contains(string(raw), "postgres://") {
		t.Fatalf("panic leaked details: %s", raw)
	}
}

func TestRequestIDMiddlewarePropagatesIdentifier(t *testing.T) {
	var seen string
	next := RequestID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = RequestIDFrom(r.Context())
		WriteJSON(w, r, http.StatusOK, map[string]string{"ok": "true"})
	}))
	req := httptest.NewRequest(http.MethodGet, "/v1/runs", nil)
	req.Header.Set("X-Request-ID", "demo-request-1")
	rec := httptest.NewRecorder()
	next.ServeHTTP(rec, req)
	if seen != "demo-request-1" {
		t.Fatalf("expected context request ID, got %q", seen)
	}
	if rec.Header().Get("X-Request-ID") != "demo-request-1" {
		t.Fatal("expected response request ID header")
	}
}

func TestServerTimeoutDefaultsAreConfigured(t *testing.T) {
	srv := WithTimeouts(&http.Server{}, ServerTimeouts{})
	if srv.ReadTimeout <= 0 || srv.ReadHeaderTimeout <= 0 || srv.WriteTimeout <= 0 || srv.IdleTimeout <= 0 {
		t.Fatalf("server timeouts must be configured: %+v", srv)
	}
	if DefaultTimeouts().HandlerTimeoutOrDefault() <= 0 {
		t.Fatal("handler timeout must be positive")
	}
}
