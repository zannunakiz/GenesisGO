package protection

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type stubProvider struct {
	outcome Outcome
	err     error
	calls   int
}

func (s *stubProvider) Decide(ctx context.Context, r *http.Request) (Outcome, error) {
	s.calls++
	return s.outcome, s.err
}

func requestWithIP(remoteAddr, forwardedFor string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/v1/runs", nil)
	req.RemoteAddr = remoteAddr
	if forwardedFor != "" {
		req.Header.Set("X-Forwarded-For", forwardedFor)
	}
	return req
}

func TestDisabledNeverCallsProvider(t *testing.T) {
	stub := &stubProvider{outcome: DenyForbidden}
	guard, err := NewWithProvider(Settings{Enabled: false}, stub)
	if err != nil {
		t.Fatal(err)
	}
	if guard.Enabled() || guard.Mode() != "disabled" {
		t.Fatal("disabled protector must not report enabled")
	}
	req := requestWithIP("203.0.113.10:4000", "")
	if guard.Check(req) != Allow {
		t.Fatal("disabled protector with quiet fallback must allow")
	}
	if stub.calls != 0 {
		t.Fatal("disabled protector must never call the hosted provider")
	}
}

func TestEnabledDenialsMapToOutcomes(t *testing.T) {
	for _, tt := range []struct {
		name string
		in   Outcome
		want Outcome
	}{
		{"allow", Allow, Allow},
		{"rate limit", DenyRateLimit, DenyRateLimit},
		{"forbidden", DenyForbidden, DenyForbidden},
	} {
		t.Run(tt.name, func(t *testing.T) {
			guard, err := NewWithProvider(Settings{Enabled: true}, &stubProvider{outcome: tt.in})
			if err != nil {
				t.Fatal(err)
			}
			if !guard.Enabled() || guard.Mode() != "enabled" {
				t.Fatal("enabled protector must report enabled")
			}
			if got := guard.Check(requestWithIP("203.0.113.11:4000", "")); got != tt.want {
				t.Fatalf("expected %d, got %d", tt.want, got)
			}
		})
	}
}

func TestProviderErrorUsesFallbackUnlessFailClosed(t *testing.T) {
	req := func() *http.Request { return requestWithIP("203.0.113.12:4000", "") }
	open, err := NewWithProvider(Settings{Enabled: true, FallbackPerMinute: 1}, &stubProvider{err: errors.New("boom")})
	if err != nil {
		t.Fatal(err)
	}
	if open.Check(req()) != Allow {
		t.Fatal("first request should allow before fallback budget is spent")
	}
	if open.Check(req()) != DenyRateLimit {
		t.Fatal("provider error should fall back to the local limiter")
	}
	closed, err := NewWithProvider(Settings{Enabled: true, FailClosed: true}, &stubProvider{err: errors.New("boom")})
	if err != nil {
		t.Fatal(err)
	}
	if closed.Check(req()) != DenyUnavailable {
		t.Fatal("fail-closed provider error must deny unavailable")
	}
}

func TestInvalidConfigurationFailsFast(t *testing.T) {
	if _, err := New(Settings{Enabled: true}); err == nil {
		t.Fatal("missing key must fail")
	}
	if _, err := New(Settings{Enabled: true, Key: "invalid"}); err == nil {
		t.Fatal("invalid key must fail")
	}
	if _, err := NewWithProvider(Settings{Enabled: true}, nil); err == nil {
		t.Fatal("nil provider must fail")
	}
	if _, err := New(Settings{Enabled: false, TrustedProxies: []string{"not-an-ip"}}); err == nil {
		t.Fatal("invalid proxy must fail")
	}
	if _, err := NewSDKProvider("invalid", nil); err == nil {
		t.Fatal("SDK provider must reject invalid key")
	}
}

func TestFallbackLimiterIsPerIPAndWindowed(t *testing.T) {
	limiter := NewFallbackLimiter(1, nil)
	base := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	current := base
	limiter.now = func() time.Time { return current }
	first := requestWithIP("203.0.113.20:4000", "")
	second := requestWithIP("203.0.113.21:4000", "")
	if limiter.Deny(first) {
		t.Fatal("first request must allow")
	}
	if !limiter.Deny(first) {
		t.Fatal("second request from same IP must deny")
	}
	if limiter.Deny(second) {
		t.Fatal("different IP must have its own budget")
	}
	current = base.Add(2 * time.Minute)
	if limiter.Deny(first) {
		t.Fatal("budget must reset in the next window")
	}
}

func TestClientIPTrustsForwardedHeadersOnlyFromProxies(t *testing.T) {
	guard, err := New(Settings{Enabled: false, TrustedProxies: []string{"10.0.0.1"}})
	if err != nil {
		t.Fatal(err)
	}
	proxied := requestWithIP("10.0.0.1:4000", "203.0.113.30, 10.0.0.1")
	if got := ClientIP(proxied, guard.fallback.proxies); got != "203.0.113.30" {
		t.Fatalf("trusted proxy header must be honoured, got %q", got)
	}
	spoofed := requestWithIP("198.51.100.9:4000", "203.0.113.99")
	if got := ClientIP(spoofed, guard.fallback.proxies); got != "198.51.100.9" {
		t.Fatalf("untrusted header must be ignored, got %q", got)
	}
	direct := requestWithIP("203.0.113.31:4000", "")
	if got := ClientIP(direct, guard.fallback.proxies); got != "203.0.113.31" {
		t.Fatalf("direct peer must be used, got %q", got)
	}
}

func TestConfigErrorsNeverIncludeSecrets(t *testing.T) {
	secret := "aj_secret-that-must-never-be-echoed-0123456789"
	if _, err := New(Settings{Enabled: false, TrustedProxies: []string{"bad"}}); err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("proxy error must not echo secrets: %v", err)
	}
	if _, err := NewSDKProvider("bad", nil); err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("provider error must not echo secrets: %v", err)
	}
}
