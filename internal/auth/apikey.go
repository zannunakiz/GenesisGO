package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Scopes guard control-plane capabilities. They form a hierarchy:
// admin implies run and read, run implies read.
const (
	ScopeRead  = "read"
	ScopeRun   = "run"
	ScopeAdmin = "admin"
)

// KeyPrefix identifies GenesisGo API secrets in logs and headers without
// revealing key material.
const KeyPrefix = "gg_"

// digestDomain separates API-key digests from other hashes.
const digestDomain = "genesisgo-api-key-v1:"

// Record persists only the digest of a secret, never the secret itself.
type Record struct {
	ID        string    `json:"id"`
	Digest    string    `json:"digest"`
	Scopes    []string  `json:"scopes"`
	CreatedAt time.Time `json:"created_at"`
	Revoked   bool      `json:"revoked"`
}

// GeneratedKey carries a one-time secret alongside its stored record.
// The secret is returned to the operator once and must never be persisted.
type GeneratedKey struct {
	Secret string
	Record Record
}

// nowFunc allows deterministic tests.
var nowFunc = time.Now

// Generate creates a random secret and its digest-only record.
func Generate(keyID string, scopes []string) (GeneratedKey, error) {
	id := strings.TrimSpace(keyID)
	if id == "" {
		return GeneratedKey{}, errors.New("key id is required")
	}
	if len([]rune(id)) > 64 {
		return GeneratedKey{}, errors.New("key id must be at most 64 characters")
	}
	normalised, err := NormaliseScopes(scopes)
	if err != nil {
		return GeneratedKey{}, err
	}
	var entropy [32]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		return GeneratedKey{}, errors.New("generate random api key")
	}
	secret := KeyPrefix + base64.RawURLEncoding.EncodeToString(entropy[:])
	return GeneratedKey{
		Secret: secret,
		Record: Record{
			ID:        id,
			Digest:    Digest(secret),
			Scopes:    normalised,
			CreatedAt: nowFunc().UTC(),
		},
	}, nil
}

// Digest returns the hex-encoded SHA-256 digest stored for a secret.
func Digest(secret string) string {
	sum := sha256.Sum256([]byte(digestDomain + secret))
	return hex.EncodeToString(sum[:])
}

// NormaliseScopes validates and deduplicates requested scopes.
func NormaliseScopes(scopes []string) ([]string, error) {
	if len(scopes) == 0 {
		return nil, errors.New("at least one scope is required")
	}
	seen := make(map[string]bool, len(scopes))
	var out []string
	for _, scope := range scopes {
		s := strings.ToLower(strings.TrimSpace(scope))
		switch s {
		case ScopeRead, ScopeRun, ScopeAdmin:
		default:
			return nil, fmt.Errorf("unsupported scope %q", scope)
		}
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out, nil
}

// HasScope reports whether a key holding haveScopes satisfies required.
func HasScope(haveScopes []string, required string) bool {
	required = strings.ToLower(strings.TrimSpace(required))
	have := make(map[string]bool, len(haveScopes))
	for _, s := range haveScopes {
		have[strings.ToLower(strings.TrimSpace(s))] = true
	}
	if have[ScopeAdmin] {
		return true
	}
	switch required {
	case ScopeRead:
		return have[ScopeRead] || have[ScopeRun]
	case ScopeRun:
		return have[ScopeRun]
	case ScopeAdmin:
		return false
	default:
		return false
	}
}

// MatchDigest compares a stored digest with a candidate secret in constant time.
func MatchDigest(storedDigest, secret string) bool {
	expected, err := hex.DecodeString(strings.TrimSpace(storedDigest))
	if err != nil || len(expected) != sha256.Size {
		return false
	}
	candidate := sha256.Sum256([]byte(digestDomain + secret))
	return subtle.ConstantTimeCompare(expected, candidate[:]) == 1
}
