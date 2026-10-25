// Package store provides SQL-first data access for GenesisGo run data.
//
// It is built on pgx/pgxpool connection pooling. Every table is scoped by
// run_id so that evidence from different runs can never be mixed.
package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// RunState is a typed run status.
type RunState string

const (
	// RunStatePending is the initial state; a run has been created but not yet
	// started by a worker.
	RunStatePending RunState = "pending"
	// RunStateRunning means a worker has picked up the run and execution is
	// in progress.
	RunStateRunning RunState = "running"
	// RunStateCompleted is a terminal state; execution finished successfully
	// and invariants were verified.
	RunStateCompleted RunState = "completed"
	// RunStateFailed is a terminal state; execution finished with an error or
	// an invariant violation.
	RunStateFailed RunState = "failed"
	// RunStateCancelled is a terminal state; the run was cancelled before or
	// during execution.
	RunStateCancelled RunState = "cancelled"
)

// terminalStates have no outgoing transitions.
var terminalStates = map[RunState]bool{
	RunStateCompleted: true,
	RunStateFailed:    true,
	RunStateCancelled: true,
}

// allowedTransitions defines the valid state machine. A run must not jump
// from pending to completed without running and verification; terminal states
// are absorbing.
var allowedTransitions = map[RunState][]RunState{
	RunStatePending:   {RunStateRunning, RunStateCancelled},
	RunStateRunning:   {RunStateCompleted, RunStateFailed, RunStateCancelled},
	RunStateCompleted: {},
	RunStateFailed:    {},
	RunStateCancelled: {},
}

// ErrInvalidTransition is returned when a requested state change is not
// permitted by the run state machine.
var ErrInvalidTransition = errors.New("invalid run state transition")

// ErrConcurrentUpdate is returned when the run's status or version no longer
// matches the expected values, indicating a concurrent worker updated it first.
var ErrConcurrentUpdate = errors.New("concurrent run state update")

// ErrRunNotFound is returned when the target run does not exist.
var ErrRunNotFound = errors.New("run not found")

// ValidateTransition reports whether moving from `from` to `to` is allowed.
func ValidateTransition(from, to RunState) error {
	if _, ok := allowedTransitions[from]; !ok {
		return fmt.Errorf("%w: unknown state %q", ErrInvalidTransition, from)
	}
	for _, allowed := range allowedTransitions[from] {
		if allowed == to {
			return nil
		}
	}
	return fmt.Errorf("%w: %q to %q", ErrInvalidTransition, from, to)
}

// IsTerminal reports whether the state has no outgoing transitions.
func IsTerminal(s RunState) bool {
	return terminalStates[s]
}

// TransitionInput carries everything needed to atomically advance a run.
type TransitionInput struct {
	RunID           string
	From            RunState
	To              RunState
	Reason          string
	ExpectedVersion int32
}

// TransitionRun atomically advances a run from `from` to `to` using optimistic
// locking on the version column. It records the transition in run_transitions,
// updates started_at/completed_at and failure/cancellation reasons, increments
// the attempt counter, and returns the updated run. The status change, audit
// insert, and timestamp updates run in a single transaction.
//
// It returns ErrInvalidTransition when the transition is not permitted,
// ErrConcurrentUpdate when another worker changed the run first, and
// ErrRunNotFound when the run does not exist.
func TransitionRun(ctx context.Context, q queryer, input TransitionInput) (*Run, error) {
	if err := ValidateTransition(input.From, input.To); err != nil {
		return nil, err
	}

	const updateQuery = `
		UPDATE runs
		SET status = $2::run_status, version = version + 1, updated_at = now(),
			started_at = COALESCE(started_at, CASE WHEN $2::text = 'running' THEN now() ELSE started_at END),
			completed_at = CASE WHEN $2::text IN ('completed', 'failed', 'cancelled') THEN now() ELSE completed_at END,
			failure_reason = CASE WHEN $2::text = 'failed' THEN $3 ELSE failure_reason END,
			cancellation_reason = CASE WHEN $2::text = 'cancelled' THEN $3 ELSE cancellation_reason END,
			attempt_count = attempt_count + 1
		WHERE id = $1 AND status = $4::run_status AND version = $5
		RETURNING id, scenario_id, status, created_at, started_at, completed_at, updated_at, failure_reason, cancellation_reason, attempt_count, version
	`
	row := q.QueryRow(ctx, updateQuery,
		input.RunID,
		input.To,
		input.Reason,
		input.From,
		input.ExpectedVersion,
	)
	run, err := scanRun(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// Distinguish between a bad from-state/version (concurrent update)
			// and a missing run.
			if _, err := GetRun(ctx, q, input.RunID); err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					return nil, ErrRunNotFound
				}
				return nil, err
			}
			return nil, ErrConcurrentUpdate
		}
		return nil, err
	}

	// Record the audit transition.
	const auditQuery = `
		INSERT INTO run_transitions (run_id, from_status, to_status, reason)
		VALUES ($1, $2, $3, $4)
	`
	if _, err := q.Exec(ctx, auditQuery, input.RunID, input.From, input.To, input.Reason); err != nil {
		return nil, err
	}

	return run, nil
}

// TransitionRun is the Store method form of TransitionRun.
func (s *Store) TransitionRun(ctx context.Context, input TransitionInput) (*Run, error) {
	return TransitionRun(ctx, s.pool, input)
}

// CancelRun is the Store method form of CancelRun.
func (s *Store) CancelRun(ctx context.Context, runID string, expectedVersion int32, reason string) (*Run, error) {
	return CancelRun(ctx, s.pool, runID, expectedVersion, reason)
}

// Ensure pgx.Tx satisfies queryer for transactional transitions.
var _ queryer = (pgx.Tx)(nil)

// CancelRun transitions a run from running or pending to cancelled. It is a
// no-op if the run is already terminal. Returns the updated run.
func CancelRun(ctx context.Context, q queryer, runID string, expectedVersion int32, reason string) (*Run, error) {
	run, err := GetRun(ctx, q, runID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrRunNotFound
		}
		return nil, err
	}
	if IsTerminal(RunState(run.Status)) {
		return nil, fmt.Errorf("%w: run %q is already terminal (%q)", ErrInvalidTransition, runID, run.Status)
	}
	from := RunState(run.Status)
	if from != RunStateRunning && from != RunStatePending {
		return nil, fmt.Errorf("%w: run %q is %q, expected %q or %q", ErrInvalidTransition, runID, run.Status, RunStateRunning, RunStatePending)
	}
	return TransitionRun(ctx, q, TransitionInput{
		RunID:           runID,
		From:            from,
		To:              RunStateCancelled,
		Reason:          reason,
		ExpectedVersion: expectedVersion,
	})
}
