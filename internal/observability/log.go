package observability

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
)

// RedactedValue replaces sensitive values in structured logs.
const RedactedValue = "[redacted]"

type correlationKey string

const (
	requestIDKey correlationKey = "request_id"
	runIDKey     correlationKey = "run_id"
)

// ParseLevel converts a configuration log level name to a slog level.
// Accepted values are debug, info, warn, and error.
func ParseLevel(name string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "debug":
		return slog.LevelDebug, nil
	case "info", "":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return slog.LevelInfo, fmt.Errorf("unknown log level %q", name)
	}
}

// NewLogger returns a JSON logger that writes structured records to out.
// Callers pass the validated application log level; debug output is only
// enabled when explicitly requested by configuration.
func NewLogger(out io.Writer, level slog.Level) *slog.Logger {
	if out == nil {
		out = io.Discard
	}
	handler := slog.NewJSONHandler(out, &slog.HandlerOptions{Level: level})
	return slog.New(handler)
}

// WithRequestID stores a request correlation identifier in the context.
func WithRequestID(ctx context.Context, requestID string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, requestIDKey, requestID)
}

// WithRunID stores a run correlation identifier in the context.
func WithRunID(ctx context.Context, runID string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, runIDKey, runID)
}

// RequestIDFrom returns the request correlation identifier in the context.
func RequestIDFrom(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if v, ok := ctx.Value(requestIDKey).(string); ok {
		return v
	}
	return ""
}

// RunIDFrom returns the run correlation identifier in the context.
func RunIDFrom(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if v, ok := ctx.Value(runIDKey).(string); ok {
		return v
	}
	return ""
}

// CorrelationAttrs returns log attributes for the identifiers in ctx.
// Only identifiers are emitted; no request body, header, or payload data.
func CorrelationAttrs(ctx context.Context) []slog.Attr {
	var attrs []slog.Attr
	if id := RequestIDFrom(ctx); id != "" {
		attrs = append(attrs, slog.String("request_id", id))
	}
	if id := RunIDFrom(ctx); id != "" {
		attrs = append(attrs, slog.String("run_id", id))
	}
	return attrs
}

// LoggerWithCorrelation returns a logger carrying the correlation identifiers
// found in ctx. It is safe to call when no identifiers are present.
func LoggerWithCorrelation(logger *slog.Logger, ctx context.Context) *slog.Logger {
	if logger == nil {
		logger = slog.Default()
	}
	attrs := CorrelationAttrs(ctx)
	if len(attrs) == 0 {
		return logger
	}
	args := make([]any, 0, len(attrs)*2)
	for _, attr := range attrs {
		args = append(args, attr)
	}
	return logger.With(args...)
}

// CorrelationMetadata returns the minimal safe correlation map suitable for
// attaching to worker contexts and backend-managed event metadata.
// It carries only opaque identifiers, never payloads or credentials.
func CorrelationMetadata(ctx context.Context) map[string]string {
	metadata := make(map[string]string, 2)
	if id := RequestIDFrom(ctx); id != "" {
		metadata["request_id"] = id
	}
	if id := RunIDFrom(ctx); id != "" {
		metadata["run_id"] = id
	}
	return metadata
}

// sensitiveKeySubstrings marks attribute names whose values must not be logged.
// Matching is case-insensitive substring matching so variants such as
// X-Api-Key, GENESISGO_API_KEY, access_token, or card_number are covered
// without allowlisting every header or payload spelling.
var sensitiveKeySubstrings = []string{
	"authorization",
	"proxy-authorization",
	"api_key",
	"apikey",
	"api-key",
	"genesisgo_api_key",
	"bootstrap_secret",
	"access_token",
	"refresh_token",
	"auth_token",
	"client_secret",
	"api_secret",
	"secret",
	"password",
	"passwd",
	"pwd",
	"cookie",
	"set-cookie",
	"token",
	"private_key",
	"privatekey",
	"card",
	"pan",
	"cvv",
	"cvc",
	"payment",
	"iban",
	"connection_url",
	"connection_string",
	"database_url",
	"dsn",
	"database_connection",
	"credentials",
	"credential",
}

// IsSensitiveKey reports whether a log attribute name may carry credentials,
// session material, or raw payment data and therefore must be redacted.
func IsSensitiveKey(key string) bool {
	normalised := strings.ToLower(strings.TrimSpace(key))
	if normalised == "" {
		return false
	}
	for _, marker := range sensitiveKeySubstrings {
		if strings.Contains(normalised, marker) {
			return true
		}
	}
	return false
}

// SafeAttr builds a log attribute, replacing the value with a fixed placeholder
// when the key is sensitive. Use it for any caller-supplied field so secrets
// cannot reach the log through a new attribute name spelling.
func SafeAttr(key string, value any) slog.Attr {
	if IsSensitiveKey(key) {
		return slog.String(key, RedactedValue)
	}
	return slog.Any(key, value)
}

// RedactAttrs returns a copy of attrs with sensitive values replaced.
// Non-sensitive attributes are preserved unchanged.
func RedactAttrs(attrs []slog.Attr) []slog.Attr {
	redacted := make([]slog.Attr, len(attrs))
	for i, attr := range attrs {
		if IsSensitiveKey(attr.Key) {
			redacted[i] = slog.String(attr.Key, RedactedValue)
		} else {
			redacted[i] = attr
		}
	}
	return redacted
}

// LogRedacted logs msg with attrs after applying the central redaction policy.
func LogRedacted(logger *slog.Logger, ctx context.Context, level slog.Level, msg string, attrs ...slog.Attr) {
	if logger == nil {
		logger = slog.Default()
	}
	logger = LoggerWithCorrelation(logger, ctx)
	redacted := RedactAttrs(attrs)
	args := make([]any, 0, len(redacted))
	for _, attr := range redacted {
		args = append(args, attr)
	}
	logger.Log(ctx, level, msg, args...)
}
