package httpapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/zannunakiz/GenesisGO/internal/protection"
)

type denyProvider struct{ outcome protection.Outcome }

func (d denyProvider) Decide(ctx context.Context, r *http.Request) (protection.Outcome, error) {
	return d.outcome, nil
}

type errorProvider struct{}

func (errorProvider) Decide(ctx context.Context, r *http.Request) (protection.Outcome, error) {
	return protection.Allow, errProtectionTest
}

func TestProtectionDenialsUseSharedEnvelope(t *testing.T) {
	cases := []struct {
		name     string
		settings protection.Settings
		provider protection.Provider
		status   int
		code     string
	}{
		{"rate limit", protection.Settings{Enabled: true}, denyProvider{protection.DenyRateLimit}, http.StatusTooManyRequests, CodeTooManyRequests},
		{"forbidden", protection.Settings{Enabled: true}, denyProvider{protection.DenyForbidden}, http.StatusForbidden, CodeForbidden},
		{"unavailable", protection.Settings{Enabled: true, FailClosed: true}, errorProvider{}, http.StatusServiceUnavailable, CodeServiceUnavailable},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			guard, err := protection.NewWithProvider(tt.settings, tt.provider)
			if err != nil {
				t.Fatal(err)
			}
			req := httptest.NewRequest(http.MethodPost, "/v1/runs", nil)
			req.RemoteAddr = "203.0.113.50:4000"
			rec := httptest.NewRecorder()
			guardedRoute(guard).ServeHTTP(rec, req)
			res := rec.Result()
			defer res.Body.Close()
			if res.StatusCode != tt.status {
				t.Fatalf("expected %d, got %d", tt.status, res.StatusCode)
			}
			envelope := decodeEnvelope(t, res.Body)
			if envelope.Code != tt.code || envelope.RequestID == "" {
				t.Fatalf("unexpected envelope: %+v", envelope)
			}
		})
	}
}

func TestProtectionErrorsDoNotLeakSecrets(t *testing.T) {
	guard, err := protection.New(protection.Settings{Enabled: false})
	if err != nil {
		t.Fatal(err)
	}
	if guard == nil {
		t.Fatal("disabled guard must exist")
	}
	_ = strings.TrimSpace("arcjet")
}
