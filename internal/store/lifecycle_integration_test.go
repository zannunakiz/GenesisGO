//go:build integration

package store

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

func lifecycleTestDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL is not set; integration test requires a live PostgreSQL")
	}
	return dsn
}

func lifecycleTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, err := NewPool(context.Background(), lifecycleTestDSN(t))
	if err != nil {
		t.Fatalf("new test pool: %v", err)
	}
	t.Cleanup(func() { pool.Close() })
	return pool
}

func lifecycleSeedScenario(t *testing.T, store *Store) string {
	t.Helper()
	ctx := context.Background()
	s, err := store.CreateScenario(ctx, "lifecycle-test", "Lifecycle Test", "tx lifecycle scenario")
	if err != nil {
		t.Fatalf("create scenario: %v", err)
	}
	return s.ID
}

func lifecycleCreateRun(t *testing.T, store *Store, scenarioID string) string {
	t.Helper()
	ctx := context.Background()
	r, err := store.CreateRun(ctx, scenarioID, string(RunStatePending))
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	if r.Status != string(RunStatePending) {
		t.Fatalf("expected pending status, got %q", r.Status)
	}
	return r.ID
}

func TestTransitionRunLifecycle(t *testing.T) {
	store := NewStore(lifecycleTestPool(t))
	ctx := context.Background()
	scenarioID := lifecycleSeedScenario(t, store)
	runID := lifecycleCreateRun(t, store, scenarioID)

	// pending -> running is allowed.
	run, err := store.TransitionRun(ctx, TransitionInput{
		RunID:          runID,
		From:           RunStatePending,
		To:             RunStateRunning,
		Reason:         "worker picked up run",
		ExpectedVersion: 1,
	})
	if err != nil {
		t.Fatalf("transition pending->running: %v", err)
	}
	if run.Status != string(RunStateRunning) {
		t.Fatalf("expected running, got %q", run.Status)
	}
	if run.Version != 2 {
		t.Fatalf("expected version 2 after transition, got %d", run.Version)
	}
	if run.AttemptCount != 1 {
		t.Fatalf("expected attempt_count 1, got %d", run.AttemptCount)
	}

	// running -> completed is allowed.
	run, err = store.TransitionRun(ctx, TransitionInput{
		RunID:          runID,
		From:           RunStateRunning,
		To:             RunStateCompleted,
		Reason:         "execution finished",
		ExpectedVersion: run.Version,
	})
	if err != nil {
		t.Fatalf("transition running->completed: %v", err)
	}
	if run.Status != string(RunStateCompleted) {
		t.Fatalf("expected completed, got %q", run.Status)
	}

	// completed is terminal: completed -> failed must be rejected.
	_, err = store.TransitionRun(ctx, TransitionInput{
		RunID:          runID,
		From:           RunStateCompleted,
		To:             RunStateFailed,
		Reason:         "should not happen",
		ExpectedVersion: run.Version,
	})
	if err == nil {
		t.Fatal("expected transition from terminal completed to fail")
	}
	if !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("expected ErrInvalidTransition, got %v", err)
	}

	// Duplicate completion: running->completed again with stale version fails.
	// Re-fetch to get the current state (completed, version 3).
	current, err := store.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	_, err = store.TransitionRun(ctx, TransitionInput{
		RunID:          runID,
		From:           RunStateRunning,
		To:             RunStateCompleted,
		Reason:         "stale duplicate",
		ExpectedVersion: current.Version, // version is now 3, but from=running doesn't match
	})
	if err == nil {
		t.Fatal("expected stale transition to fail")
	}
}

func TestTransitionRunConcurrentUpdate(t *testing.T) {
	store := NewStore(lifecycleTestPool(t))
	ctx := context.Background()
	scenarioID := lifecycleSeedScenario(t, store)
	runID := lifecycleCreateRun(t, store, scenarioID)

	// First transition succeeds: pending -> running, version 1 -> 2.
	_, err := store.TransitionRun(ctx, TransitionInput{
		RunID:          runID,
		From:           RunStatePending,
		To:             RunStateRunning,
		Reason:         "first worker",
		ExpectedVersion: 1,
	})
	if err != nil {
		t.Fatalf("first transition: %v", err)
	}

	// Second transition with stale version (1) should fail with ErrConcurrentUpdate.
	_, err = store.TransitionRun(ctx, TransitionInput{
		RunID:          runID,
		From:           RunStatePending,
		To:             RunStateCancelled,
		Reason:         "stale worker",
		ExpectedVersion: 1,
	})
	if err == nil {
		t.Fatal("expected concurrent update to fail")
	}
	if !errors.Is(err, ErrConcurrentUpdate) {
		t.Fatalf("expected ErrConcurrentUpdate, got %v", err)
	}
}

func TestTransitionRunMissingRun(t *testing.T) {
	store := NewStore(lifecycleTestPool(t))
	ctx := context.Background()
	_, err := store.TransitionRun(ctx, TransitionInput{
		RunID:          "00000000-0000-0000-0000-000000000000",
		From:           RunStatePending,
		To:             RunStateRunning,
		Reason:         "no such run",
		ExpectedVersion: 1,
	})
	if err == nil {
		t.Fatal("expected run not found error")
	}
	if !errors.Is(err, ErrRunNotFound) {
		t.Fatalf("expected ErrRunNotFound, got %v", err)
	}
}
