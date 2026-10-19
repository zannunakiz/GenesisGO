package httpapi

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/zannunakiz/GenesisGO/internal/protection"
)

func guardedRoute(guard *protection.Protector) http.Handler {
	return RequestID(Protection(guard)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		WriteJSON(w, r, http.StatusOK, map[string]string{"ok": "true"})
	})))
}

func TestProtectionDisabledAllowsWithFallback(t *testing.T) {
	guard, err := protection.New(protection.Settings{Enabled: false, FallbackPerMinute: 1})
	if err != nil {
		t.Fatal(err)
	}
	first := httptest.NewRequest(http.MethodPost, "/v1/runs", nil)
	first.RemoteAddr = "203.0.113.40:4000"
	rec := httptest.NewRecorder()
	guardedRoute(guard).ServeHTTP(rec, first)
	if res := rec.Result(); res.StatusCode != http.StatusOK {
		res.Body.Close()
		t.Fatalf("expected 200, got %d", res.StatusCode)
	} else {
		res.Body.Close()
	}
	second := httptest.NewRequest(http.MethodPost, "/v1/runs", nil)
	second.RemoteAddr = "203.0.113.40:4000"
	rec = httptest.NewRecorder()
	guardedRoute(guard).ServeHTTP(rec, second)
	res := rec.Result()
	defer res.Body.Close()
	if res.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("expected 429, got %d", res.StatusCode)
	}
	if envelope := decodeEnvelope(t, res.Body); envelope.Code != CodeTooManyRequests {
		t.Fatalf("unexpected envelope: %+v", envelope)
	}
	if res.Header.Get("X-Request-ID") == "" {
		t.Fatal("protection denials must carry correlation ID")
	}
}
