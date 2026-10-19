package httpapi

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type samplePayload struct {
	Name   string   `json:"name"`
	Status string   `json:"status"`
	Tags   []string `json:"tags"`
}

func testLimits() Limits {
	return Limits{MaxBodyBytes: 1024, MaxStringLength: 8, MaxListItems: 2}
}

func decodeEnvelope(t *testing.T, body io.Reader) ErrorEnvelope {
	t.Helper()
	var envelope ErrorEnvelope
	if err := json.NewDecoder(body).Decode(&envelope); err != nil {
		t.Fatalf("decode error envelope: %v", err)
	}
	return envelope
}

func TestDecodeMalformedJSONUsesSharedEnvelope(t *testing.T) {
	var dst samplePayload
	req := httptest.NewRequest(http.MethodPost, "/v1/runs", strings.NewReader("{invalid"))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	if DecodeJSON(rec, req, testLimits(), &dst) {
		t.Fatal("expected malformed JSON to be rejected")
	}
	res := rec.Result()
	defer res.Body.Close()
	if res.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", res.StatusCode)
	}
	envelope := decodeEnvelope(t, res.Body)
	if envelope.Code != CodeInvalidJSON || envelope.RequestID == "" {
		t.Fatalf("unexpected envelope: %+v", envelope)
	}
	if res.Header.Get("X-Request-ID") != envelope.RequestID {
		t.Fatal("request ID header must match envelope")
	}
}

func TestDecodeRejectsUnsupportedMediaType(t *testing.T) {
	var dst samplePayload
	req := httptest.NewRequest(http.MethodPost, "/v1/runs", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "text/plain")
	rec := httptest.NewRecorder()

	if DecodeJSON(rec, req, testLimits(), &dst) {
		t.Fatal("expected unsupported media type to be rejected")
	}
	res := rec.Result()
	defer res.Body.Close()
	if res.StatusCode != http.StatusUnsupportedMediaType {
		t.Fatalf("expected 415, got %d", res.StatusCode)
	}
	if envelope := decodeEnvelope(t, res.Body); envelope.Code != CodeUnsupportedMediaType {
		t.Fatalf("unexpected envelope: %+v", envelope)
	}
}

func TestDecodeRejectsOversizedBody(t *testing.T) {
	var dst samplePayload
	big := strings.Repeat("a", 2048)
	req := httptest.NewRequest(http.MethodPost, "/v1/runs", strings.NewReader(`{"name":"`+big+`"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	if DecodeJSON(rec, req, testLimits(), &dst) {
		t.Fatal("expected oversized body to be rejected")
	}
	res := rec.Result()
	defer res.Body.Close()
	if res.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d", res.StatusCode)
	}
	if envelope := decodeEnvelope(t, res.Body); envelope.Code != CodePayloadTooLarge {
		t.Fatalf("unexpected envelope: %+v", envelope)
	}
}

func TestDecodeRejectsUnknownFieldsAndTrailingJSON(t *testing.T) {
	for _, body := range []string{`{"unknown":1}`, `{} {}`} {
		var dst samplePayload
		req := httptest.NewRequest(http.MethodPost, "/v1/runs", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		if DecodeJSON(rec, req, testLimits(), &dst) {
			t.Fatalf("expected body %q to be rejected", body)
		}
		res := rec.Result()
		res.Body.Close()
		if res.StatusCode != http.StatusBadRequest {
			t.Fatalf("expected 400 for %q, got %d", body, res.StatusCode)
		}
	}
}
