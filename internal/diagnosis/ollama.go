// Package diagnosis provides optional local AI-assisted diagnosis for run
// reports. See provider.go for the interface contract.
package diagnosis

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// ollamaChatRequest mirrors the Ollama /api/chat request body (non-streaming).
type ollamaChatRequest struct {
	Model    string                 `json:"model"`
	Messages []ollamaMessage        `json:"messages"`
	Stream   bool                   `json:"stream"`
	Options  map[string]interface{} `json:"options,omitempty"`
}

// ollamaMessage is a single chat message.
type ollamaMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// ollamaChatResponse mirrors the Ollama /api/chat response body.
type ollamaChatResponse struct {
	Done      bool          `json:"done"`
	Message   ollamaMessage `json:"message"`
	CreatedAt string        `json:"created_at,omitempty"`
}

// ollamaDiagnosis is the structured JSON we ask the model to return.
type ollamaDiagnosis struct {
	Summary     string   `json:"summary"`
	Suggestions []string `json:"suggestions,omitempty"`
}

// OllamaProvider calls a local Ollama server over HTTP to produce an optional
// diagnosis. It applies network timeout, bounded input/output, cancellation,
// and graceful unavailability so a missing Ollama never changes verdicts.
type OllamaProvider struct {
	baseURL    string
	model      string
	httpClient *http.Client
}

// NewOllamaProvider builds an OllamaProvider from validated config values.
// An empty baseURL/model produces a provider whose Diagnose returns
// (nil, errProviderUnavailable) so callers can mark diagnosis unavailable.
func NewOllamaProvider(baseURL, model string, timeout time.Duration) *OllamaProvider {
	if timeout <= 0 {
		timeout = defaultOllamaTimeout
	}
	return &OllamaProvider{
		baseURL:    baseURL,
		model:      model,
		httpClient: &http.Client{Timeout: timeout},
	}
}

const (
	defaultOllamaTimeout  = 15 * time.Second
	defaultMaxInputBytes  = 64 * 1024
	defaultMaxOutputBytes = 256 * 1024
	errMsgUnavailable     = "ollama provider unavailable"
)

// Diagnose sends the minimum required evidence to Ollama and returns a parsed
// diagnosis result. On any failure it returns (nil, err) so the caller can mark
// diagnosis unavailable without affecting invariant verdicts.
func (p *OllamaProvider) Diagnose(ctx context.Context, in Input) (*Result, error) {
	if p == nil || p.baseURL == "" || p.model == "" || p.httpClient == nil {
		return nil, errors.New(errMsgUnavailable)
	}

	payload, err := marshalInput(in)
	if err != nil {
		return nil, fmt.Errorf("marshal diagnosis input: %w", err)
	}
	if len(payload) > defaultMaxInputBytes {
		payload = payload[:defaultMaxInputBytes]
	}

	body := ollamaChatRequest{
		Model: p.model,
		Messages: []ollamaMessage{
			{Role: "system", Content: systemPrompt()},
			{Role: "user", Content: string(payload)},
		},
		Stream: false,
		Options: map[string]interface{}{
			"num_predict": 512,
			"temperature": 0.2,
		},
	}

	reqBody, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal ollama request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.baseURL+"/api/chat", bytes.NewReader(reqBody))
	if err != nil {
		return nil, fmt.Errorf("create ollama request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("ollama request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("ollama returned status %d", resp.StatusCode)
	}

	limited := io.LimitReader(resp.Body, defaultMaxOutputBytes)
	raw, err := io.ReadAll(limited)
	if err != nil {
		return nil, fmt.Errorf("read ollama response: %w", err)
	}

	var parsed ollamaChatResponse
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil, fmt.Errorf("parse ollama response: %w", err)
	}

	content := strings.TrimSpace(parsed.Message.Content)
	if content == "" {
		return nil, errors.New("ollama returned empty response")
	}

	// Best-effort parse of structured JSON; fall back to the whole content as
	// the summary so a non-JSON model response is still usable.
	result := &Result{
		Summary:   &content,
		Model:     &p.model,
		CreatedAt: time.Now().UTC(),
	}
	if diag, err := parseDiagnosis([]byte(content)); err == nil {
		result.Summary = &diag.Summary
		result.Suggestions = diag.Suggestions
	}

	return result, nil
}

// marshalInput renders the diagnosis input as compact JSON.
func marshalInput(in Input) ([]byte, error) {
	return json.Marshal(in)
}

// systemPrompt returns the system instruction given to Ollama. It keeps the
// model within bounds: hypotheses only, no tool use, no state changes.
func systemPrompt() string {
	return `You are a diagnostic assistant for GenesisGo, a reliability testing tool.
You receive ONLY observed run evidence: invariant failures, event timeline, fault points, and safe before/after state diffs.
Your output is a hypothesis, never a ground-truth verdict. Do not change run state, invariant outcomes, or trigger execution.
Return JSON only, with fields: summary (string), suggestions (array of strings, optional).
Keep it concise, cite evidence by run/scenario IDs, and do not invent evidence IDs or raw payloads.`
}

// parseDiagnosis attempts to decode structured JSON from the model output.
func parseDiagnosis(raw []byte) (ollamaDiagnosis, error) {
	var d ollamaDiagnosis
	if err := json.Unmarshal(raw, &d); err != nil {
		return d, err
	}
	return d, nil
}
