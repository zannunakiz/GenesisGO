package store

import (
	"errors"
	"testing"
)

func TestValidateTransitionAllowed(t *testing.T) {
	cases := []struct {
		name string
		from RunState
		to   RunState
	}{
		{"pending to running", RunStatePending, RunStateRunning},
		{"pending to cancelled", RunStatePending, RunStateCancelled},
		{"running to completed", RunStateRunning, RunStateCompleted},
		{"running to failed", RunStateRunning, RunStateFailed},
		{"running to cancelled", RunStateRunning, RunStateCancelled},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := ValidateTransition(tc.from, tc.to); err != nil {
				t.Fatalf("expected allowed transition %q -> %q, got %v", tc.from, tc.to, err)
			}
		})
	}
}

func TestValidateTransitionRejected(t *testing.T) {
	cases := []struct {
		name string
		from RunState
		to   RunState
	}{
		{"pending to completed (no execution)", RunStatePending, RunStateCompleted},
		{"running to pending (backwards)", RunStateRunning, RunStatePending},
		{"completed to running (terminal)", RunStateCompleted, RunStateRunning},
		{"failed to running (terminal)", RunStateFailed, RunStateRunning},
		{"cancelled to running (terminal)", RunStateCancelled, RunStateRunning},
		{"completed to failed (terminal)", RunStateCompleted, RunStateFailed},
		{"unknown state", RunState("bogus"), RunStateRunning},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateTransition(tc.from, tc.to)
			if err == nil {
				t.Fatalf("expected rejected transition %q -> %q, got nil", tc.from, tc.to)
			}
			if !errors.Is(err, ErrInvalidTransition) {
				t.Fatalf("expected ErrInvalidTransition, got %v", err)
			}
		})
	}
}

func TestIsTerminal(t *testing.T) {
	if !IsTerminal(RunStateCompleted) {
		t.Error("completed should be terminal")
	}
	if !IsTerminal(RunStateFailed) {
		t.Error("failed should be terminal")
	}
	if !IsTerminal(RunStateCancelled) {
		t.Error("cancelled should be terminal")
	}
	if IsTerminal(RunStatePending) {
		t.Error("pending should not be terminal")
	}
	if IsTerminal(RunStateRunning) {
		t.Error("running should not be terminal")
	}
}
