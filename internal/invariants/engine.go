// Package invariants implements a deterministic business invariant engine.
// It evaluates run evidence against hard-coded rules and returns structured
// verdicts (pass/fail/error) with expected value, actual value, and evidence
// references. No user-supplied expressions or code are ever executed.
package invariants

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"
)

// Status is the outcome of evaluating a single invariant rule.
type Status string

const (
	// StatusPass means the evidence satisfies the invariant.
	StatusPass Status = "pass"
	// StatusFail means the evidence violates the invariant.
	StatusFail Status = "fail"
	// StatusError means the rule could not be evaluated (e.g. malformed input).
	StatusError Status = "error"
)

// EffectRecord is a single observed business effect in a run.
type EffectRecord struct {
	ID             string    `json:"id"`
	RunID          string    `json:"run_id"`
	EffectType     string    `json:"effect_type"`
	AggregateID    string    `json:"aggregate_id"`
	IdempotencyKey *string   `json:"idempotency_key,omitempty"`
	AmountCents    *int64    `json:"amount_cents,omitempty"`
	Currency       string    `json:"currency"`
	RecordedAt     time.Time `json:"recorded_at"`
}

// Evidence is the deterministic input an invariant evaluates against. It is
// built from persisted side-effect records for a single run; evidence from
// different runs is never mixed.
type Evidence struct {
	RunID  string         `json:"run_id"`
	Effects []EffectRecord `json:"effects"`
}

// Result is a structured invariant verdict.
type Result struct {
	InvariantKey string    `json:"invariant_key"`
	Rule         string    `json:"rule"`
	Status       Status    `json:"status"`
	Expected     string    `json:"expected"`
	Actual       string    `json:"actual"`
	Evidence     []string  `json:"evidence"` // effect IDs referenced by this verdict
	EvaluatedAt  time.Time `json:"evaluated_at"`
}

// Rule evaluates evidence against a single business invariant. Implementations
// must be deterministic and must not execute user-supplied code.
type Rule interface {
	// Key returns the invariant key this rule implements.
	Key() string
	// Evaluate returns a structured verdict for the given evidence.
	Evaluate(ctx context.Context, ev Evidence) (*Result, error)
}

// Engine holds registered rules and evaluates evidence against all of them.
type Engine struct {
	rules map[string]Rule
}

// NewEngine builds an empty engine. Register rules before evaluating.
func NewEngine() *Engine {
	return &Engine{rules: make(map[string]Rule)}
}

// Register adds a rule to the engine. Registering the same key twice replaces
// the previous rule.
func (e *Engine) Register(r Rule) {
	if r == nil {
		return
	}
	e.rules[r.Key()] = r
}

// EvaluateAll runs every registered rule against the evidence and returns
// verdicts sorted by invariant key for stable output.
func (e *Engine) EvaluateAll(ctx context.Context, ev Evidence) []*Result {
	results := make([]*Result, 0, len(e.rules))
	for _, r := range e.rules {
		res, err := r.Evaluate(ctx, ev)
		if err != nil {
			res = &Result{
				InvariantKey: r.Key(),
				Rule:         r.Key(),
				Status:       StatusError,
				Expected:     "rule evaluation",
				Actual:       err.Error(),
				EvaluatedAt:  time.Now().UTC(),
			}
		}
		results = append(results, res)
	}
	sort.Slice(results, func(i, j int) bool { return results[i].InvariantKey < results[j].InvariantKey })
	return results
}

// Evaluate runs a single registered rule against the evidence.
func (e *Engine) Evaluate(ctx context.Context, key string, ev Evidence) (*Result, error) {
	r, ok := e.rules[key]
	if !ok {
		return nil, fmt.Errorf("no rule registered for invariant %q", key)
	}
	return r.Evaluate(ctx, ev)
}

// AllRules returns the registered rule keys in sorted order.
func (e *Engine) AllRules() []string {
	keys := make([]string, 0, len(e.rules))
	for k := range e.rules {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// Ensure time is used (EvaluatedAt) and errors are handled.
var _ = errors.New
var _ = sort.Strings
