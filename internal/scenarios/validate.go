package scenarios

import (
	"errors"
	"fmt"
	"strings"
)

// Validation error sentinels.
var (
	ErrInvalidScenario  = errors.New("invalid scenario")
	ErrInvalidKey       = errors.New("invalid scenario key")
	ErrInvalidVersion   = errors.New("invalid scenario version")
	ErrInvalidStep      = errors.New("invalid scenario step")
	ErrInvalidParameter = errors.New("invalid scenario parameter")
	ErrInvalidInvariant = errors.New("invalid scenario invariant")
)

// Validation limits.
const (
	maxNameLength     = 128
	maxDescriptionLen = 1024
	maxSteps          = 200
	minEvents         = 1
	maxEvents         = 1000
	minRetries        = 0
	maxRetries        = 10
	minTimeoutMs      = 1000
	maxTimeoutMs      = 300000
	minConcurrency    = 1
	maxConcurrency    = 16
)

// Validate checks a scenario for structural soundness and bounded parameters.
// It rejects unknown step/fault/invariant types, oversized parameters, and
// missing required fields. It does not execute any scenario content.
func Validate(s Scenario) error {
	if s.Key == "" || !validKey(s.Key) {
		return fmt.Errorf("%w: key must be lowercase alphanumeric with underscores, 1-64 chars", ErrInvalidKey)
	}
	if s.Version < 1 {
		return fmt.Errorf("%w: version must be a positive integer", ErrInvalidVersion)
	}
	if strings.TrimSpace(s.Name) == "" {
		return fmt.Errorf("%w: name is required", ErrInvalidScenario)
	}
	if len([]rune(s.Name)) > maxNameLength {
		return fmt.Errorf("%w: name must be at most %d characters", ErrInvalidScenario, maxNameLength)
	}
	if len([]rune(s.Description)) > maxDescriptionLen {
		return fmt.Errorf("%w: description must be at most %d characters", ErrInvalidScenario, maxDescriptionLen)
	}
	if len(s.Steps) == 0 {
		return fmt.Errorf("%w: at least one step is required", ErrInvalidScenario)
	}
	if len(s.Steps) > maxSteps {
		return fmt.Errorf("%w: at most %d steps allowed", ErrInvalidScenario, maxSteps)
	}
	seenStepIDs := make(map[string]bool, len(s.Steps))
	for i, step := range s.Steps {
		if err := validateStep(step, i); err != nil {
			return err
		}
		if seenStepIDs[step.ID] {
			return fmt.Errorf("%w: duplicate step id %q at index %d", ErrInvalidStep, step.ID, i)
		}
		seenStepIDs[step.ID] = true
	}
	if err := validateParameters(s.Parameters); err != nil {
		return err
	}
	seenInvariantKeys := make(map[string]bool, len(s.Invariants))
	for i, inv := range s.Invariants {
		if err := validateInvariant(inv, i); err != nil {
			return err
		}
		if seenInvariantKeys[inv.Key] {
			return fmt.Errorf("%w: duplicate invariant key %q at index %d", ErrInvalidInvariant, inv.Key, i)
		}
		seenInvariantKeys[inv.Key] = true
	}
	return nil
}

func validateStep(step Step, index int) error {
	if strings.TrimSpace(step.ID) == "" {
		return fmt.Errorf("%w: step at index %d missing id", ErrInvalidStep, index)
	}
	if len([]rune(step.ID)) > 64 {
		return fmt.Errorf("%w: step id %q must be at most 64 characters", ErrInvalidStep, step.ID)
	}
	if !allowedStepTypes[step.Type] {
		return fmt.Errorf("%w: step %q has unknown type %q", ErrInvalidStep, step.ID, step.Type)
	}
	if step.Type == "publish" && strings.TrimSpace(step.EventID) == "" {
		return fmt.Errorf("%w: publish step %q requires event_id", ErrInvalidStep, step.ID)
	}
	if step.Type == "deliver" && strings.TrimSpace(step.EventID) == "" {
		return fmt.Errorf("%w: deliver step %q requires event_id", ErrInvalidStep, step.ID)
	}
	if step.Fault != "" && !allowedFaultTypes[step.Fault] {
		return fmt.Errorf("%w: step %q has unknown fault %q", ErrInvalidStep, step.ID, step.Fault)
	}
	if step.DelayMs < 0 || step.DelayMs > maxTimeoutMs {
		return fmt.Errorf("%w: step %q delay_ms out of bounds", ErrInvalidStep, step.ID)
	}
	return nil
}

func validateParameters(p Parameters) error {
	if p.MaxEvents < minEvents || p.MaxEvents > maxEvents {
		return fmt.Errorf("%w: max_events must be between %d and %d", ErrInvalidParameter, minEvents, maxEvents)
	}
	if p.MaxRetries < minRetries || p.MaxRetries > maxRetries {
		return fmt.Errorf("%w: max_retries must be between %d and %d", ErrInvalidParameter, minRetries, maxRetries)
	}
	if p.TimeoutMs < minTimeoutMs || p.TimeoutMs > maxTimeoutMs {
		return fmt.Errorf("%w: timeout_ms must be between %d and %d", ErrInvalidParameter, minTimeoutMs, maxTimeoutMs)
	}
	if p.Concurrency < minConcurrency || p.Concurrency > maxConcurrency {
		return fmt.Errorf("%w: concurrency must be between %d and %d", ErrInvalidParameter, minConcurrency, maxConcurrency)
	}
	return nil
}

func validateInvariant(inv Invariant, index int) error {
	if strings.TrimSpace(inv.Key) == "" {
		return fmt.Errorf("%w: invariant at index %d missing key", ErrInvalidInvariant, index)
	}
	if !allowedInvariantTypes[inv.Type] {
		return fmt.Errorf("%w: invariant %q has unknown type %q", ErrInvalidInvariant, inv.Key, inv.Type)
	}
	return nil
}
