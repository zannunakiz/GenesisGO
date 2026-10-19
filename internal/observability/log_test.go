package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
)

func TestParseLevelAcceptsConfiguredValues(t *testing.T) {
	for _, name := range []string{"debug", "info", "warn", "error"} {
		if _, err := ParseLevel(name); err != nil {
			t.Fatalf("expected level %q to parse, got %v", name, err)
		}
	}
	if _, err := ParseLevel("trace"); err == nil {
		t.Fatal("expected unknown level to fail")
	}
}

func TestLoggerEmitsStructuredJSON(t *testing.T) {
	var buf bytes.Buffer
	logger, err := ParseLevel("info")
	if err != nil {
		t.Fatal(err)
	}
	jsonLogger := NewLogger(&buf, logger)
	jsonLogger.Info("request completed", slog.String("request_id", "req-123"), slog.Int("status", 200))

	var record map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &record); err != nil {
		t.Fatalf("log output must be valid JSON: %v\n%s", err, buf.String())
	}
	if record["msg"] != "request completed" {
		t.Fatalf("unexpected log record: %v", record)
	}
	if record["request_id"] != "req-123" {
		t.Fatalf("correlation identifier missing: %v", record)
	}
}

func TestCorrelationIdentifiersPropagateThroughContext(t *testing.T) {
	ctx := context.Background()
	ctx = WithRequestID(ctx, "req-abc")
	ctx = WithRunID(ctx, "run-xyz")

	if got := RequestIDFrom(ctx); got != "req-abc" {
		t.Fatalf("expected request ID, got %q", got)
	}
	if got := RunIDFrom(ctx); got != "run-xyz" {
		t.Fatalf("expected run ID, got %q", got)
	}

	metadata := CorrelationMetadata(ctx)
	if metadata["request_id"] != "req-abc" || metadata["run_id"] != "run-xyz" {
		t.Fatalf("event metadata must carry only correlation IDs, got %v", metadata)
	}
	if len(metadata) != 2 {
		t.Fatalf("metadata must not carry payload data, got %v", metadata)
	}
}

func TestRedactionPolicyCoversSecretsAndPaymentFields(t *testing.T) {
	secrets := map[string]string{
		"Authorization":           "Bearer super-secret-token",
		"X-Api-Key":               "arcjet-secret-key",
		"Cookie":                  "session=secret-session",
		"client_secret":           "oauth-secret",
		"password":                "hunter2",
		"card_number":             "4111111111111111",
		"payment_method_token":    "tok-secret",
		"private_key":             "-----BEGIN PRIVATE KEY-----",
		"raw_payment_payload":     `{"pan":"4111111111111111"}`,
		"database_connection_url": "postgres://user:secret@localhost/db",
	}
	for key := range secrets {
		if !IsSensitiveKey(key) {
			t.Fatalf("expected key %q to be treated as sensitive", key)
		}
	}
	for _, safe := range []string{"request_id", "run_id", "method", "path", "status", "duration_ms", "scenario_id"} {
		if IsSensitiveKey(safe) {
			t.Fatalf("expected key %q to remain loggable", safe)
		}
	}
}

func TestLogRedactedNeverWritesSecretValues(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(&buf, slog.LevelDebug)
	ctx := WithRequestID(context.Background(), "req-secret-check")

	markers := []string{
		"Bearer marker-auth-secret-001",
		"marker-api-key-002",
		"marker-cookie-003",
		"marker-card-4111111111111111",
		"gg_marker-bootstrap-secret-004",
		"GENESISGO_API_KEY=gg_marker-bootstrap-secret-004",
	}
	LogRedacted(logger, ctx, slog.LevelInfo, "request completed",
		slog.String("method", "POST"),
		slog.String("Authorization", markers[0]),
		slog.String("X-Api-Key", markers[1]),
		slog.String("Cookie", markers[2]),
		slog.String("card_number", markers[3]),
		slog.String("GENESISGO_API_KEY", markers[4]),
		slog.String("api_key", markers[5]),
		slog.String("request_id", "req-secret-check"),
	)

	output := buf.String()
	for _, marker := range markers {
		if strings.Contains(output, marker) {
			t.Fatalf("log output leaked sensitive marker %q: %s", marker, output)
		}
	}
	if !strings.Contains(output, RedactedValue) {
		t.Fatalf("expected redaction placeholder in output: %s", output)
	}
	if !strings.Contains(output, "req-secret-check") {
		t.Fatalf("expected correlation identifier in output: %s", output)
	}
	var record map[string]any
	for _, line := range bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n")) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var entry map[string]any
		if err := json.Unmarshal(line, &entry); err != nil {
			t.Fatalf("log line must be valid JSON: %v", err)
		}
		record = entry
	}
	if record["request_id"] != "req-secret-check" {
		t.Fatalf("structured correlation missing: %v", record)
	}
}

func TestLoggerWithCorrelationKeepsIdentifiers(t *testing.T) {
	var buf bytes.Buffer
	base := NewLogger(&buf, slog.LevelInfo)
	ctx := WithRequestID(WithRunID(context.Background(), "run-7"), "req-7")
	LoggerWithCorrelation(base, ctx).Info("worker progress", slog.String("phase", "verification"))

	var record map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &record); err != nil {
		t.Fatalf("log output must be valid JSON: %v", err)
	}
	if record["request_id"] != "req-7" || record["run_id"] != "run-7" {
		t.Fatalf("correlation identifiers must be consistent: %v", record)
	}
}
