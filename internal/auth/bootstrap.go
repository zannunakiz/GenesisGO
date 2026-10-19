package auth

import (
	"errors"
	"fmt"
	"strings"
)

// BootstrapOptions controls local development key creation. The command
// generating the secret must print it once to stdout and must never write
// it to a tracked file, log, or report.
type BootstrapOptions struct {
	KeyID  string
	Scopes []string
	Force  bool
}

// BootstrapResult reports whether a new secret was issued.
type BootstrapResult struct {
	Secret  string
	KeyID   string
	Scopes  []string
	Rotated bool
}

// Bootstrap creates the first local key, or rotates it when Force is set.
// Existing non-revoked keys are left untouched unless rotation is requested.
func Bootstrap(store Store, opts BootstrapOptions) (BootstrapResult, error) {
	if store == nil {
		return BootstrapResult{}, errors.New("api key store is required")
	}
	id := strings.TrimSpace(opts.KeyID)
	if id == "" {
		return BootstrapResult{}, errors.New("key id is required")
	}
	scopes, err := NormaliseScopes(opts.Scopes)
	if err != nil {
		return BootstrapResult{}, err
	}
	for _, existing := range store.List() {
		if existing.ID == id && !existing.Revoked && !opts.Force {
			return BootstrapResult{KeyID: existing.ID, Scopes: existing.Scopes, Rotated: false}, nil
		}
	}
	generated, err := Generate(id, scopes)
	if err != nil {
		return BootstrapResult{}, err
	}
	if err := store.Upsert(generated.Record); err != nil {
		return BootstrapResult{}, err
	}
	if opts.Force {
		// Upsert already replaced the digest for this ID; mark the result so
		// operators know the previous secret stopped working.
		return BootstrapResult{Secret: generated.Secret, KeyID: generated.Record.ID, Scopes: generated.Record.Scopes, Rotated: true}, nil
	}
	return BootstrapResult{Secret: generated.Secret, KeyID: generated.Record.ID, Scopes: generated.Record.Scopes, Rotated: len(store.List()) > 1}, nil
}

// DescribeKeyID validates operator-supplied key IDs for CLI commands.
func DescribeKeyID(keyID string) (string, error) {
	id := strings.TrimSpace(keyID)
	if id == "" {
		return "", errors.New("key id is required")
	}
	if len([]rune(id)) > 64 {
		return "", fmt.Errorf("key id must be at most 64 characters")
	}
	return id, nil
}
