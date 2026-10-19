package httpapi

import (
	"context"
	"net/http"
	"strings"

	"github.com/zannunakiz/GenesisGO/internal/auth"
)

type authContextKey string

const (
	authKeyIDKey  authContextKey = "auth_key_id"
	authScopesKey authContextKey = "auth_scopes"
)

// AuthenticatedKeyID returns the API key identifier stored in the context.
func AuthenticatedKeyID(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if v, ok := ctx.Value(authKeyIDKey).(string); ok {
		return v
	}
	return ""
}

// AuthenticatedScopes returns the scopes stored in the context.
func AuthenticatedScopes(ctx context.Context) []string {
	if ctx == nil {
		return nil
	}
	if v, ok := ctx.Value(authScopesKey).([]string); ok {
		return append([]string(nil), v...)
	}
	return nil
}

func contextWithAuth(ctx context.Context, record auth.Record) context.Context {
	ctx = context.WithValue(ctx, authKeyIDKey, record.ID)
	ctx = context.WithValue(ctx, authScopesKey, append([]string(nil), record.Scopes...))
	return ctx
}

// bearerSecret extracts a candidate secret without logging or echoing it.
// It accepts Authorization: Bearer <secret> and X-API-Key: <secret>.
func bearerSecret(r *http.Request) string {
	if r == nil {
		return ""
	}
	if header := strings.TrimSpace(r.Header.Get("Authorization")); header != "" {
		parts := strings.SplitN(header, " ", 2)
		if len(parts) == 2 && strings.EqualFold(strings.TrimSpace(parts[0]), "bearer") {
			if secret := strings.TrimSpace(parts[1]); secret != "" {
				return secret
			}
		}
	}
	return strings.TrimSpace(r.Header.Get("X-API-Key"))
}

// RequireAuth protects control-plane routes. Health endpoints stay open and
// must not be wrapped. Missing or unknown secrets yield 401; a valid key
// without the required scope yields 403. Responses never echo key material.
func RequireAuth(store auth.Store, requiredScope string) func(http.Handler) http.Handler {
	scope := strings.ToLower(strings.TrimSpace(requiredScope))
	if scope == "" {
		scope = auth.ScopeRead
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if store == nil {
				Internal(w, r, nil)
				return
			}
			secret := bearerSecret(r)
			if secret == "" {
				Unauthorized(w, r, "Authentication is required.")
				return
			}
			record, ok := store.Lookup(auth.Digest(secret))
			if !ok {
				Unauthorized(w, r, "Authentication is required.")
				return
			}
			if !auth.HasScope(record.Scopes, scope) {
				Forbidden(w, r, "This API key does not have the required scope.")
				return
			}
			next.ServeHTTP(w, r.WithContext(contextWithAuth(r.Context(), record)))
		})
	}
}
