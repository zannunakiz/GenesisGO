package protection

import (
	"context"
	"net/http"
	"time"

	"github.com/arcjet/arcjet-go"
)

// SDKProvider is the hosted Arcjet adapter. It applies Shield and a
// token-bucket rate limit to abuse-sensitive routes. Prompt-injection and
// sensitive-information checks are intentionally not applied here: they are
// only appropriate for AI-bound text, which this request path does not carry.
type SDKProvider struct {
	client *arcjet.Client
}

// NewSDKProvider builds the Arcjet client from the official Go SDK using its
// supported Config, Shield, and TokenBucket constructors.
func NewSDKProvider(key string, proxies []string) (*SDKProvider, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}
	client, err := arcjet.NewClient(arcjet.Config{
		Key:     key,
		Proxies: proxies,
		Rules: []arcjet.Rule{
			arcjet.Shield(arcjet.ShieldOptions{Mode: arcjet.ModeLive}),
			arcjet.TokenBucket(arcjet.TokenBucketOptions{
				Mode:       arcjet.ModeLive,
				RefillRate: 60,
				Interval:   time.Minute,
				Capacity:   120,
			}),
		},
	})
	if err != nil {
		return nil, errProtectionMisconfigured
	}
	return &SDKProvider{client: client}, nil
}

// Decide evaluates a request with Arcjet. Denials map to stable outcomes;
// provider errors are returned so the caller can fail open to the fallback
// limiter or fail closed. Errors never include key material.
func (p *SDKProvider) Decide(ctx context.Context, r *http.Request) (Outcome, error) {
	if p == nil || p.client == nil || r == nil {
		return Allow, errProtectionMisconfigured
	}
	decision, err := p.client.Protect(ctx, r)
	if err != nil {
		return Allow, err
	}
	if decision.IsErrored() {
		if decision.Err() != nil {
			return Allow, decision.Err()
		}
		return Allow, errProtectionMisconfigured
	}
	if decision.IsDenied() {
		if decision.Reason.IsRateLimit() {
			return DenyRateLimit, nil
		}
		return DenyForbidden, nil
	}
	return Allow, nil
}

// Close releases SDK resources.
func (p *SDKProvider) Close(ctx context.Context) error {
	if p == nil || p.client == nil {
		return nil
	}
	return p.client.Close(ctx)
}
