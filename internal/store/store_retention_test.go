//go:build integration

package store

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func seedScenarioRetention(t *testing.T, pool *pgxpool.Pool, ctx context.Context, key string) string {
	t.Helper()
	var id string
	if err := pool.QueryRow(ctx, `INSERT INTO scenarios (key, name) VALUES ($1, $2) RETURNING id`, key, key).Scan(&id); err != nil {
		t.Fatalf("seed scenario: %v", err)
	}
	return id
}

func seedRunRetention(t *testing.T, pool *pgxpool.Pool, ctx context.Context, scenarioID, status string, createdAt time.Time) string {
	t.Helper()
	var id string
	err := pool.QueryRow(ctx, `
		INSERT INTO runs (scenario_id, status, created_at, updated_at, completed_at)
		VALUES ($1, $2, $3, $3, CASE WHEN $2 IN ('completed','failed','cancelled') THEN $3 ELSE NULL END)
		RETURNING id`, scenarioID, status, createdAt).Scan(&id)
	if err != nil {
		t.Fatalf("seed run: %v", err)
	}
	return id
}

func TestCleanupOldDataRespectsRetentionBoundaries(t *testing.T) {
	pool := newTestPool(t)
	st := NewStore(pool)
	ctx := context.Background()

	cutoff := time.Now().UTC().Add(-48 * time.Hour)
	old := time.Now().UTC().Add(-120 * time.Hour)
	recent := time.Now().UTC().Add(-1 * time.Hour)

	sa := seedScenarioRetention(t, pool, ctx, "alpha")
	sb := seedScenarioRetention(t, pool, ctx, "beta")

	// alpha: 5 old terminal runs; maxRuns=2 keeps the 2 newest, deletes 3.
	for i := 0; i < 5; i++ {
		runID := seedRunRetention(t, pool, ctx, sa, "completed", old)
		if _, err := pool.Exec(ctx, `INSERT INTO event_attempts (run_id, event_id, attempt_number) VALUES ($1, $2, 1)`, runID, "evt-"+runID); err != nil {
			t.Fatalf("seed attempt: %v", err)
		}
		if _, err := pool.Exec(ctx, `INSERT INTO audit_logs (run_id, action, actor_key_id, detail) VALUES ($1, $2, $3, $4)`, runID, "run.created", "test-key", "scenario_key=alpha"); err != nil {
			t.Fatalf("seed audit: %v", err)
		}
	}
	// beta: 1 old terminal run is kept (latest run of its scenario).
	seedRunRetention(t, pool, ctx, sb, "completed", old)
	// gamma: an old active (pending) run is never deleted.
	seedRunRetention(t, pool, ctx, sb, "pending", old)
	// delta: a recent terminal run is kept (within retention).
	seedRunRetention(t, pool, ctx, sa, "completed", recent)

	result, err := st.CleanupOldData(ctx, cutoff, 2)
	if err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	if result.RunsDeleted != 3 {
		t.Fatalf("expected 3 runs deleted, got %d", result.RunsDeleted)
	}

	var count int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM runs`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 5 { // alpha 2 + beta terminal 1 + beta pending 1 + delta 1
		t.Fatalf("expected 5 runs remain, got %d", count)
	}
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM event_attempts`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 { // attempts for the 2 kept alpha runs
		t.Fatalf("expected 2 event attempts remain, got %d", count)
	}
	// Audit records for deleted runs keep their history with run_id NULL.
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM audit_logs WHERE run_id IS NULL`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Fatalf("expected 3 orphaned audit records, got %d", count)
	}
}
