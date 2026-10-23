package diagnosis

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// stubOllama returns an httptest server that answers /api/chat.
type stubOllama struct {
	handler http.HandlerFunc
}

func (s *stubOllama) Server() *httptest.Server {
	return httptest.NewServer(s.handler)
}

func newSuccessStub() *stubOllama {
	return &stubOllama{handler: func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/chat" {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		resp := ollamaChatResponse{
			Done: true,
			Message: ollamaMessage{
				Role:    "assistant",
				Content: `{"summary":"Likely duplicate delivery caused the failed idempotency invariant.","suggestions":["Add idempotency keys","Enforce unique constraints"]}`,
			},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}}
}

func newUnavailableStub() *stubOllama {
	return &stubOllama{handler: func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "internal error", http.StatusInternalServerError)
	}}
}

func newInvalidJSONStub() *stubOllama {
	return &stubOllama{handler: func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`not-json`))
	}}
}

func newEmptyContentStub() *stubOllama {
	return &stubOllama{handler: func(w http.ResponseWriter, r *http.Request) {
		resp := ollamaChatResponse{Done: true, Message: ollamaMessage{Role: "assistant", Content: ""}}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}}
}

func newOversizedStub() *stubOllama {
	return &stubOllama{handler: func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(strings.Repeat("x", 300*1024)))
	}}
}

func TestOllamaSuccessfulDiagnosis(t *testing.T) {
	srv := newSuccessStub().Server()
	defer srv.Close()

	p := NewOllamaProvider(srv.URL, "test-model", 10*time.Second)
	in := Input{
		RunID:            "run-1",
		ScenarioID:       "scenario-1",
		OverallVerdict:   "fail",
		FailedInvariants: []InvariantSummary{{Key: "idempotency", Verdict: "failed"}},
	}
	ctx := context.Background()

	res, err := p.Diagnose(ctx, in)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res == nil {
		t.Fatal("expected non-nil result")
	}
	if res.Summary == nil || *res.Summary == "" {
		t.Fatal("expected non-empty summary")
	}
	if res.Model == nil || *res.Model != "test-model" {
		t.Fatalf("expected model test-model, got %v", res.Model)
	}
	if len(res.Suggestions) != 2 {
		t.Fatalf("expected 2 suggestions, got %d", len(res.Suggestions))
	}
}

func TestOllamaUnavailableReturnsNil(t *testing.T) {
	srv := newUnavailableStub().Server()
	defer srv.Close()

	p := NewOllamaProvider(srv.URL, "test-model", 10*time.Second)
	res, err := p.Diagnose(context.Background(), Input{RunID: "run-1"})
	if err == nil {
		t.Fatal("expected error for unavailable ollama")
	}
	if res != nil {
		t.Fatalf("expected nil result on unavailable, got %v", res)
	}
}

func TestOllamaEmptyConfigUnavailable(t *testing.T) {
	p := NewOllamaProvider("", "", 10*time.Second)
	res, err := p.Diagnose(context.Background(), Input{})
	if err == nil {
		t.Fatal("expected error for empty config")
	}
	if res != nil {
		t.Fatal("expected nil result for empty config")
	}
}

func TestOllamaInvalidJSONReturnsError(t *testing.T) {
	srv := newInvalidJSONStub().Server()
	defer srv.Close()

	p := NewOllamaProvider(srv.URL, "test-model", 10*time.Second)
	res, err := p.Diagnose(context.Background(), Input{RunID: "run-1"})
	if err == nil {
		t.Fatal("expected error for invalid JSON response")
	}
	if res != nil {
		t.Fatal("expected nil result for invalid JSON response")
	}
}

func TestOllamaEmptyContentReturnsError(t *testing.T) {
	srv := newEmptyContentStub().Server()
	defer srv.Close()

	p := NewOllamaProvider(srv.URL, "test-model", 10*time.Second)
	res, err := p.Diagnose(context.Background(), Input{RunID: "run-1"})
	if err == nil {
		t.Fatal("expected error for empty content")
	}
	if res != nil {
		t.Fatal("expected nil result for empty content")
	}
}

func TestOllamaTimeout(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release // block until the test releases it
	}))
	defer srv.Close()
	defer close(release)

	p := NewOllamaProvider(srv.URL, "test-model", 50*time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := p.Diagnose(ctx, Input{RunID: "run-1"})
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected timeout/cancellation error")
	}
	if elapsed > 5*time.Second {
		t.Fatalf("timeout took too long: %v", elapsed)
	}
	low := strings.ToLower(err.Error())
	if !strings.Contains(low, "timeout") && !strings.Contains(low, "canceled") && !strings.Contains(low, "deadline") {
		t.Fatalf("expected timeout-related error, got %v", err)
	}
}

func TestOllamaProviderImplementsInterface(t *testing.T) {
	var _ DiagnosisProvider = (*OllamaProvider)(nil)
}

func TestMarshalInputProducesJSON(t *testing.T) {
	in := Input{RunID: "run-1", ScenarioID: "scenario-1"}
	b, err := marshalInput(in)
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}
	var m map[string]interface{}
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("output not valid JSON: %v", err)
	}
	if m["run_id"] != "run-1" {
		t.Fatal("expected run_id in marshalled input")
	}
}
