package httpapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/zannunakiz/GenesisGO/internal/auth"
)

func authedHandler(store auth.Store, scope string) http.Handler {
	return RequestID(RequireAuth(store, scope)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if AuthenticatedKeyID(r.Context()) == "" {
			tHelperError(w, r)
			return
		}
		WriteJSON(w, r, http.StatusOK, map[string]string{"key": AuthenticatedKeyID(r.Context())})
	})))
}

func tHelperError(w http.ResponseWriter, r *http.Request) {
	Internal(w, r, nil)
}

func seedStore(t *testing.T, scopes ...string) (auth.Store, string) {
	t.Helper()
	generated, err := auth.Generate("local-admin", scopes)
	if err != nil {
		t.Fatal(err)
	}
	store, err := auth.NewMemoryStore(generated.Record)
	if err != nil {
		t.Fatal(err)
	}
	return store, generated.Secret
}

func TestAuthMiddlewareAcceptsBearerAndAPIKeyHeaders(t *testing.T) {
	for _, setup := range []func(r *http.Request, secret string){
		func(r *http.Request, secret string) { r.Header.Set("Authorization", "Bearer "+secret) },
		func(r *http.Request, secret string) { r.Header.Set("X-API-Key", secret) },
	} {
		store, secret := seedStore(t, auth.ScopeAdmin)
		req := httptest.NewRequest(http.MethodPost, "/v1/runs", nil)
		setup(req, secret)
		rec := httptest.NewRecorder()
		authedHandler(store, auth.ScopeRun).ServeHTTP(rec, req)
		res := rec.Result()
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("expected authenticated request to succeed, got %d", res.StatusCode)
		}
	}
}

func TestAuthMiddlewareRejectsMissingAndInvalidKeys(t *testing.T) {
	store, secret := seedStore(t, auth.ScopeAdmin)
	cases := []struct {
		name   string
		mutate func(r *http.Request)
	}{
		{"missing", func(r *http.Request) {}},
		{"invalid", func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+secret+"-wrong") }},
		{"malformed scheme", func(r *http.Request) { r.Header.Set("Authorization", "Token "+secret) }},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/v1/runs", nil)
			tt.mutate(req)
			rec := httptest.NewRecorder()
			authedHandler(store, auth.ScopeRead).ServeHTTP(rec, req)
			res := rec.Result()
			defer res.Body.Close()
			if res.StatusCode != http.StatusUnauthorized {
				t.Fatalf("expected 401, got %d", res.StatusCode)
			}
			envelope := decodeEnvelope(t, res.Body)
			if envelope.Code != CodeUnauthorized {
				t.Fatalf("unexpected envelope: %+v", envelope)
			}
			if res.Header.Get("X-Request-ID") == "" {
				t.Fatal("auth errors must carry correlation ID")
			}
		})
	}
}

func TestAuthMiddlewareEnforcesScopes(t *testing.T) {
	store, secret := seedStore(t, auth.ScopeRead)
	req := httptest.NewRequest(http.MethodPost, "/v1/runs", nil)
	req.Header.Set("Authorization", "Bearer "+secret)
	rec := httptest.NewRecorder()
	authedHandler(store, auth.ScopeRun).ServeHTTP(rec, req)
	res := rec.Result()
	defer res.Body.Close()
	if res.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", res.StatusCode)
	}
	if envelope := decodeEnvelope(t, res.Body); envelope.Code != CodeForbidden {
		t.Fatalf("unexpected envelope: %+v", envelope)
	}

	req = httptest.NewRequest(http.MethodGet, "/v1/reports", nil)
	req.Header.Set("Authorization", "Bearer "+secret)
	rec = httptest.NewRecorder()
	authedHandler(store, auth.ScopeRead).ServeHTTP(rec, req)
	res = rec.Result()
	res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("expected read scope to satisfy read route, got %d", res.StatusCode)
	}
}

func TestAuthMiddlewareRejectsRevokedKeys(t *testing.T) {
	generated, err := auth.Generate("local-admin", []string{"admin"})
	if err != nil {
		t.Fatal(err)
	}
	store, err := auth.NewMemoryStore(generated.Record)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Revoke("local-admin"); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "/v1/reports", nil)
	req.Header.Set("Authorization", "Bearer "+generated.Secret)
	rec := httptest.NewRecorder()
	authedHandler(store, auth.ScopeRead).ServeHTTP(rec, req)
	res := rec.Result()
	defer res.Body.Close()
	if res.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected revoked key to yield 401, got %d", res.StatusCode)
	}
}

func TestAuthErrorsNeverEchoSecrets(t *testing.T) {
	store, secret := seedStore(t, auth.ScopeRead)
	req := httptest.NewRequest(http.MethodPost, "/v1/runs", nil)
	req.Header.Set("Authorization", "Bearer "+secret+"-wrong")
	rec := httptest.NewRecorder()
	authedHandler(store, auth.ScopeRun).ServeHTTP(rec, req)
	res := rec.Result()
	body := readAll(t, res)
	res.Body.Close()
	if strings.Contains(string(body), secret) {
		t.Fatal("auth error must not echo key material")
	}
}
