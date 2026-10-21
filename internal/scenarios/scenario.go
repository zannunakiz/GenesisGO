// Package scenarios defines versioned, deterministic fault scenarios and their
// built-in fixtures. Scenarios are loaded from committed JSON fixture files;
// arbitrary shell commands or code are never accepted from API requests.
package scenarios

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Scenario is a versioned, deterministic fault scenario. It defines the steps
// the orchestrator executes, bounded execution parameters, expected business
// invariants, and an optional deterministic seed.
type Scenario struct {
	Key         string     `json:"key"`
	Version     int        `json:"version"`
	Name        string     `json:"name"`
	Description string     `json:"description,omitempty"`
	Steps       []Step     `json:"steps"`
	Parameters  Parameters `json:"parameters"`
	Invariants  []Invariant `json:"invariants"`
	Seed        int64      `json:"seed,omitempty"`
}

// Step is a single bounded action executed within a scenario run.
type Step struct {
	ID        string            `json:"id"`
	Type      string            `json:"type"` // publish, deliver, ack, crash, restart, delay
	EventID   string            `json:"event_id,omitempty"`
	Attempt   int               `json:"attempt,omitempty"`
	Fault     string            `json:"fault,omitempty"` // duplicate, out_of_order, retry, crash_after_commit
	DelayMs   int               `json:"delay_ms,omitempty"`
	Payload   map[string]any    `json:"payload,omitempty"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

// Parameters bounds scenario execution to keep runs safe and predictable.
type Parameters struct {
	MaxEvents   int `json:"max_events"`   // max events per scenario run
	MaxRetries  int `json:"max_retries"`  // max redelivery attempts
	TimeoutMs   int `json:"timeout_ms"`   // per-step timeout
	Concurrency int `json:"concurrency"`  // max concurrent workers
}

// Invariant is an expected business rule the scenario must satisfy.
type Invariant struct {
	Key       string            `json:"key"`
	Type      string            `json:"type"` // no_duplicate_effect, no_negative_inventory, idempotency_key_unique, allowed_order_transition
	EffectType string           `json:"effect_type,omitempty"`
	Aggregate  string           `json:"aggregate,omitempty"`
	Details   map[string]any    `json:"details,omitempty"`
}

// allowlisted step types.
var allowedStepTypes = map[string]bool{
	"publish":  true,
	"deliver":  true,
	"ack":      true,
	"crash":    true,
	"restart":  true,
	"delay":    true,
}

// allowlisted fault types.
var allowedFaultTypes = map[string]bool{
	"":                    true, // no fault
	"duplicate":           true,
	"out_of_order":        true,
	"retry":               true,
	"crash_after_commit":  true,
}

// allowlisted invariant types.
var allowedInvariantTypes = map[string]bool{
	"no_duplicate_effect":          true,
	"no_negative_inventory":        true,
	"idempotency_key_unique":       true,
	"allowed_order_transition":     true,
}

// keyPattern validates scenario keys (lowercase, alphanumeric + underscores).
func validKey(key string) bool {
	if key == "" || len(key) > 64 {
		return false
	}
	for _, r := range key {
		if !((r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_') {
			return false
		}
	}
	return true
}

// defaultParameters returns safe bounded defaults.
func defaultParameters() Parameters {
	return Parameters{
		MaxEvents:   10,
		MaxRetries:  3,
		TimeoutMs:   30000,
		Concurrency: 1,
	}
}

// loadDir reads all *.json files from dir and returns parsed scenarios.
func loadDir(dir string) ([]Scenario, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var out []Scenario
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		var s Scenario
		decoder := json.NewDecoder(strings.NewReader(string(raw)))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&s); err != nil {
			return nil, fmt.Errorf("parse %s: %w", entry.Name(), err)
		}
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

// MarshalJSON returns the canonical, sorted JSON encoding of a scenario so the
// same input always produces the same plan (deterministic output).
func (s Scenario) MarshalJSON() ([]byte, error) {
	type alias Scenario
	a := alias(s)
	return json.Marshal(a)
}

// Equal reports whether two scenarios are semantically identical.
func (s Scenario) Equal(other Scenario) bool {
	a, err := json.Marshal(s)
	if err != nil {
		return false
	}
	b, err := json.Marshal(other)
	if err != nil {
		return false
	}
	return string(a) == string(b)
}
