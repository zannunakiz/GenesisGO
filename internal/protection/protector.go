package protection

import (
	"context"
	"net/http"
)

// Outcome is the protection decision for a request.
type Outcome int

const (
	// Allow lets the request proceed.
	Allow Outcome = iota
	// DenyRateLimit rejects the request as too many requests.
	DenyRateLimit
	// DenyForbidden rejects the request as suspicious/forbidden.
	DenyForbidden
	// DenyUnavailable rejects the request because protection failed closed.
	DenyUnavailable
)

// Provider evaluates a request with an external protection service.
// Implementations must never include key material in errors.
type Provider interface {
	Decide(ctx context.Context, r *http.Request) (Outcome, error)
}

// Settings configures the protector. Enabled=false leaves the product
// working with local validation and the documented fallback limiter.
type Settings struct {
	Enabled           bool
	Key               string
	TrustedProxies    []string
	FallbackPerMinute int
	FailClosed        bool
}

const defaultFallbackPerMinute = 120

// Protector applies Arcjet (when enabled) with a per-process fallback limiter.
type Protector struct {
	enabled    bool
	failClosed bool
	provider   Provider
	fallback   *FallbackLimiter
}

// New builds a protector backed by the real Arcjet SDK when enabled.
// When disabled it returns a fallback-only protector and never contacts Arcjet.
// When enabled, missing/invalid configuration returns an error so startup fails
// instead of silently reporting Arcjet as active.
func New(s Settings) (*Protector, error) {
	proxies, err := parseTrustedProxies(s.TrustedProxies)
	if err != nil {
		return nil, err
	}
	fallback := NewFallbackLimiter(s.FallbackPerMinute, proxies)
	if !s.Enabled {
		return &Protector{enabled: false, fallback: fallback}, nil
	}
	if err := validateKey(s.Key); err != nil {
		return nil, err
	}
	provider, err := NewSDKProvider(s.Key, s.TrustedProxies)
	if err != nil {
		return nil, err
	}
	return &Protector{enabled: true, failClosed: s.FailClosed, provider: provider, fallback: fallback}, nil
}

// NewWithProvider builds a protector with an injected provider for tests.
// A nil provider with enabled=true returns an error.
func NewWithProvider(s Settings, provider Provider) (*Protector, error) {
	proxies, err := parseTrustedProxies(s.TrustedProxies)
	if err != nil {
		return nil, err
	}
	fallback := NewFallbackLimiter(s.FallbackPerMinute, proxies)
	if !s.Enabled {
		return &Protector{enabled: false, fallback: fallback}, nil
	}
	if provider == nil {
		return nil, errProtectionMisconfigured
	}
	return &Protector{enabled: true, failClosed: s.FailClosed, provider: provider, fallback: fallback}, nil
}

// Enabled reports whether hosted Arcjet protection is active.
func (p *Protector) Enabled() bool {
	if p == nil {
		return false
	}
	return p.enabled && p.provider != nil
}

// Mode returns a health-safe label without exposing configuration values.
func (p *Protector) Mode() string {
	if p.Enabled() {
		return "enabled"
	}
	return "disabled"
}

// Check evaluates a request. It always applies the fallback limiter;
// when enabled it consults Arcjet first and falls back on provider errors
// unless FailClosed is set.
func (p *Protector) Check(r *http.Request) Outcome {
	if p == nil || p.fallback == nil {
		return Allow
	}
	if p.Enabled() {
		outcome, err := p.provider.Decide(r.Context(), r)
		if err == nil {
			return outcome
		}
		if p.failClosed {
			return DenyUnavailable
		}
		if p.fallback.Deny(r) {
			return DenyRateLimit
		}
		return Allow
	}
	if p.fallback.Deny(r) {
		return DenyRateLimit
	}
	return Allow
}

// Close releases provider resources. Safe on a disabled protector.
func (p *Protector) Close(ctx context.Context) error {
	if p == nil || p.provider == nil {
		return nil
	}
	if closer, ok := p.provider.(interface{ Close(context.Context) error }); ok {
		return closer.Close(ctx)
	}
	return nil
}
