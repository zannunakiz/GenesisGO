package httpapi

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/zannunakiz/GenesisGO/internal/observability"
)

func TestRequestLoggerEmitsJSONWithCorrelationID(t *testing.T) {
	var buf bytes.Buffer
	logger := observability.NewLogger(&buf, slog.LevelInfo)

	handler := RequestID(RequestLogger(logger)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if observability.RequestIDFrom(r.Context()) == "" {
			t.Error("expected observability request ID in handler context")
		}
		if RequestIDFrom(r.Context()) == "" {
			t.Error("expected transport request ID in handler context")
		}
		WriteJSON(w, r, http.StatusOK, map[string]string{"ok": "true"})
	})))

	req := httptest.NewRequest(http.MethodGet, "/v1/runs", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	res := rec.Result()
	defer res.Body.Close()
	responseID := res.Header.Get("X-Request-ID")
	if responseID == "" {
		t.Fatal("expected response request ID header")
	}

	line := bytes.TrimSpace(buf.Bytes())
	var record map[string]any
	if err := json.Unmarshal(line, &record); err != nil {
		t.Fatalf("request log must be valid JSON: %v\n%s", err, buf.String())
	}
	if record["request_id"] != responseID {
		t.Fatalf("log correlation must match response: log=%v response=%q", record["request_id"], responseID)
	}
	if record["method"] != http.MethodGet || record["path"] != "/v1/runs" {
		t.Fatalf("request log must carry safe request summary: %v", record)
	}
	for _, forbidden := range []string{"Cookie", "Authorization", "secret"} {
		if _, ok := record[forbidden]; ok {
			t.Fatalf("request log must not contain %q: %v", forbidden, record)
		}
	}
}

func TestRequestLoggerDoesNotWriteSensitiveMaterial(t *testing.T) {
	var buf bytes.Buffer
	logger := observability.NewLogger(&buf, slog.LevelInfo)
	secret := "marker-secret-cookie-999"

	handler := RequestID(RequestLogger(logger)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		WriteJSON(w, r, http.StatusOK, map[string]string{"ok": "true"})
	})))

	req := httptest.NewRequest(http.MethodPost, "/v1/runs?token="+secret, strings.NewReader(`{"name":"demo"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+secret)
	req.Header.Set("Cookie", "session="+secret)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	output := buf.String()
	if strings.Contains(output, secret) {
		t.Fatalf("request log leaked sensitive material: %s", output)
	}
	var record map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &record); err != nil {
		t.Fatalf("request log must be valid JSON: %v", err)
	}
	if _, ok := record["request_id"]; !ok {
		t.Fatalf("request log must retain correlation ID: %v", record)
	}
}
