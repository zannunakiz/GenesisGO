package scenarios

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// builtinKeys are the five scenarios the validation gate requires.
var builtinKeys = []string{
	"duplicate_delivery",
	"out_of_order",
	"retry_after_timeout",
	"worker_restart",
	"crash_after_commit_before_ack",
}

func fixtureDir(t *testing.T) string {
	t.Helper()
	dir, err := filepath.Abs("fixtures")
	if err != nil {
		t.Fatalf("abs fixtures dir: %v", err)
	}
	return dir
}

func TestLoadAllBuiltinFixtures(t *testing.T) {
	loader := NewLoader(fixtureDir(t))
	scenarios, err := loader.Load()
	if err != nil {
		t.Fatalf("load fixtures: %v", err)
	}
	if len(scenarios) != len(builtinKeys) {
		t.Fatalf("expected %d scenarios, got %d: %v", len(builtinKeys), len(scenarios), scenarios)
	}
	for _, key := range builtinKeys {
		s, ok := scenarios[key]
		if !ok {
			t.Fatalf("missing built-in scenario %q", key)
		}
		if s.Version < 1 {
			t.Fatalf("scenario %q has invalid version %d", key, s.Version)
		}
		if len(s.Steps) == 0 {
			t.Fatalf("scenario %q has no steps", key)
		}
		if len(s.Invariants) == 0 {
			t.Fatalf("scenario %q has no invariants", key)
		}
		if s.Seed < 0 {
			t.Fatalf("scenario %q has negative seed %d", key, s.Seed)
		}
	}
}

func TestFixtureFileMatchesKey(t *testing.T) {
	dir := fixtureDir(t)
	for _, key := range builtinKeys {
		path := filepath.Join(dir, key+".json")
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("missing fixture file for %q: %v", key, err)
		}
	}
}

func TestValidateAcceptsAllBuiltins(t *testing.T) {
	loader := NewLoader(fixtureDir(t))
	scenarios, err := loader.Load()
	if err != nil {
		t.Fatalf("load fixtures: %v", err)
	}
	for _, key := range builtinKeys {
		if err := Validate(*scenarios[key]); err != nil {
			t.Fatalf("validate %q: %v", key, err)
		}
	}
}

func TestValidateRejectsBadKey(t *testing.T) {
	s := Scenario{Key: "Bad Key!", Version: 1, Name: "x", Steps: []Step{{ID: "s1", Type: "publish", EventID: "e1"}}}
	if err := Validate(s); err == nil {
		t.Fatal("expected validation error for bad key")
	}
}

func TestValidateRejectsZeroVersion(t *testing.T) {
	s := Scenario{Key: "ok_key", Version: 0, Name: "x", Steps: []Step{{ID: "s1", Type: "publish", EventID: "e1"}}}
	if err := Validate(s); err == nil {
		t.Fatal("expected validation error for zero version")
	}
}

func TestValidateRejectsUnknownStepType(t *testing.T) {
	s := Scenario{
		Key:     "ok_key",
		Version: 1,
		Name:    "x",
		Steps:   []Step{{ID: "s1", Type: "exec_shell", EventID: "e1"}},
	}
	if err := Validate(s); err == nil {
		t.Fatal("expected validation error for unknown step type")
	}
}

func TestValidateRejectsOversizedParameters(t *testing.T) {
	s := Scenario{
		Key:      "ok_key",
		Version:  1,
		Name:     "x",
		Steps:    []Step{{ID: "s1", Type: "publish", EventID: "e1"}},
		Parameters: Parameters{MaxEvents: 99999, MaxRetries: 3, TimeoutMs: 15000, Concurrency: 1},
	}
	if err := Validate(s); err == nil {
		t.Fatal("expected validation error for oversized max_events")
	}
}

func TestValidateRejectsDuplicateStepIDs(t *testing.T) {
	s := Scenario{
		Key:     "ok_key",
		Version: 1,
		Name:    "x",
		Steps: []Step{
			{ID: "s1", Type: "publish", EventID: "e1"},
			{ID: "s1", Type: "deliver", EventID: "e1"},
		},
	}
	if err := Validate(s); err == nil {
		t.Fatal("expected validation error for duplicate step ids")
	}
}

func TestDeterministicMarshal(t *testing.T) {
	loader := NewLoader(fixtureDir(t))
	scenarios, err := loader.Load()
	if err != nil {
		t.Fatalf("load fixtures: %v", err)
	}
	for _, key := range builtinKeys {
		s := *scenarios[key]
		a, err1 := json.Marshal(s)
		b, err2 := json.Marshal(s)
		if err1 != nil || err2 != nil {
			t.Fatalf("marshal %q: %v %v", key, err1, err2)
		}
		if string(a) != string(b) {
			t.Fatalf("scenario %q is not deterministic across marshal calls", key)
		}
		// Re-parse and compare to ensure stable round-trip.
		var rt Scenario
		if err := json.Unmarshal(a, &rt); err != nil {
			t.Fatalf("unmarshal %q: %v", key, err)
		}
		if !s.Equal(rt) {
			t.Fatalf("scenario %q round-trip changed", key)
		}
	}
}
