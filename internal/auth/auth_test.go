package auth

import (
	"strings"
	"testing"
)

func TestGenerateStoresDigestOnly(t *testing.T) {
	generated, err := Generate("local-admin", []string{"admin"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(generated.Secret, KeyPrefix) {
		t.Fatalf("secret must carry the key prefix, got %q", generated.Secret)
	}
	if generated.Record.Digest == generated.Secret || strings.Contains(generated.Record.Digest, generated.Secret) {
		t.Fatal("record must not contain the plaintext secret")
	}
	if generated.Record.Digest != Digest(generated.Secret) {
		t.Fatal("record digest must match the secret")
	}
	if !MatchDigest(generated.Record.Digest, generated.Secret) {
		t.Fatal("stored digest must verify the secret")
	}
	if MatchDigest(generated.Record.Digest, generated.Secret+"x") {
		t.Fatal("tampered secret must not verify")
	}
	if len(generated.Record.Scopes) != 1 || generated.Record.Scopes[0] != ScopeAdmin {
		t.Fatalf("unexpected scopes: %v", generated.Record.Scopes)
	}
}

func TestScopeHierarchy(t *testing.T) {
	if !HasScope([]string{ScopeAdmin}, ScopeRun) || !HasScope([]string{ScopeAdmin}, ScopeRead) {
		t.Fatal("admin must imply run and read")
	}
	if !HasScope([]string{ScopeRun}, ScopeRead) {
		t.Fatal("run must imply read")
	}
	if HasScope([]string{ScopeRead}, ScopeRun) || HasScope([]string{ScopeRun}, ScopeAdmin) {
		t.Fatal("lower scopes must not imply higher scopes")
	}
	if HasScope([]string{ScopeRead}, "unknown") {
		t.Fatal("unknown scopes must be rejected")
	}
	if _, err := NormaliseScopes([]string{"owner"}); err == nil {
		t.Fatal("unsupported scope must be rejected")
	}
}

func TestMemoryStoreRotationAndRevocation(t *testing.T) {
	store, err := NewMemoryStore()
	if err != nil {
		t.Fatal(err)
	}
	first, err := Generate("local-admin", []string{"admin"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Upsert(first.Record); err != nil {
		t.Fatal(err)
	}
	if _, ok := store.Lookup(Digest(first.Secret)); !ok {
		t.Fatal("expected first secret to authenticate")
	}

	second, err := Generate("local-admin", []string{"run"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Upsert(second.Record); err != nil {
		t.Fatal(err)
	}
	if _, ok := store.Lookup(Digest(first.Secret)); ok {
		t.Fatal("rotation must invalidate the previous secret")
	}
	record, ok := store.Lookup(Digest(second.Secret))
	if !ok || len(record.Scopes) != 1 || record.Scopes[0] != ScopeRun {
		t.Fatalf("rotated record must be active with new scopes: %+v", record)
	}

	if err := store.Revoke("local-admin"); err != nil {
		t.Fatal(err)
	}
	if _, ok := store.Lookup(Digest(second.Secret)); ok {
		t.Fatal("revoked secret must not authenticate")
	}
}

func TestFileStorePersistsDigestsOnly(t *testing.T) {
	path := t.TempDir() + "/api-keys.json"
	store, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	generated, err := Generate("local-admin", []string{"admin"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Upsert(generated.Record); err != nil {
		t.Fatal(err)
	}
	reloaded, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := reloaded.Lookup(Digest(generated.Secret)); !ok {
		t.Fatal("reloaded digest must authenticate")
	}
	raw := readFile(t, path)
	if strings.Contains(raw, generated.Secret) {
		t.Fatal("key file must never contain the plaintext secret")
	}
	if strings.Contains(raw, KeyPrefix) {
		t.Fatal("key file must store digests only")
	}
}

func TestBootstrapLeavesExistingKeyAlone(t *testing.T) {
	store, err := NewMemoryStore()
	if err != nil {
		t.Fatal(err)
	}
	first, err := Bootstrap(store, BootstrapOptions{KeyID: "local-admin", Scopes: []string{"admin"}})
	if err != nil || first.Secret == "" {
		t.Fatalf("expected first bootstrap to issue a secret: %+v %v", first, err)
	}
	second, err := Bootstrap(store, BootstrapOptions{KeyID: "local-admin", Scopes: []string{"admin"}})
	if err != nil || second.Secret != "" {
		t.Fatalf("expected repeat bootstrap to leave the key alone: %+v %v", second, err)
	}
	rotated, err := Bootstrap(store, BootstrapOptions{KeyID: "local-admin", Scopes: []string{"run"}, Force: true})
	if err != nil || rotated.Secret == "" || !rotated.Rotated {
		t.Fatalf("expected forced bootstrap to rotate: %+v %v", rotated, err)
	}
	if _, ok := store.Lookup(Digest(first.Secret)); ok {
		t.Fatal("rotation must invalidate the previous secret")
	}
	if _, ok := store.Lookup(Digest(rotated.Secret)); !ok {
		t.Fatal("rotated secret must authenticate")
	}
}
