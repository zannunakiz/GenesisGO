//go:build integration

package store

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func testDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL is not set; integration test requires a live PostgreSQL")
	}
	return dsn
}

func newTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	pool, err := NewPool(ctx, testDSN(t))
	if err != nil {
		t.Fatalf("new test pool: %v", err)
	}
	t.Cleanup(func() {
		pool.Close()
	})
	return pool
}

func TestNewPoolRejectsEmptyURL(t *testing.T) {
	_, err := NewPool(context.Background(), "")
	if err == nil {
		t.Fatal("expected error for empty database url")
	}
}

func TestStoreTransactionRollback(t *testing.T) {
	pool := newTestPool(t)
	store := NewStore(pool)
	ctx := context.Background()

	// Seed a scenario outside any transaction.
	scenario, err := store.CreateScenario(ctx, "rollback-test", "Rollback Test", "tx rollback scenario")
	if err != nil {
		t.Fatalf("create scenario: %v", err)
	}

	runID := ""
	commitErr := store.WithTx(ctx, func(tx pgx.Tx) error {
		run, err := CreateRun(ctx, tx, scenario.ID, "running")
		if err != nil {
			return err
		}
		runID = run.ID

		// Insert a run job inside the transaction.
		_, err = CreateRunJob(ctx, tx, run.ID, "publish", "queued", []byte("{}"))
		if err != nil {
			return err
		}

		// Simulate a failure mid-transaction: the caller returns an error.
		return errors.New("simulated failure")
	})
	if commitErr == nil || commitErr.Error() != "simulated failure" {
		t.Fatalf("expected simulated failure to propagate, got %v", commitErr)
	}

	// The run must not be visible after rollback.
	run, err := store.GetRun(ctx, runID)
	if err == nil {
		t.Fatalf("expected GetRun to fail after rollback, got run %+v", run)
	}
	if runID == "" {
		t.Fatal("runID was not set")
	}

	// A fresh transaction that commits successfully should persist.
	var committedID string
	commitErr = store.WithTx(ctx, func(tx pgx.Tx) error {
		run, err := CreateRun(ctx, tx, scenario.ID, "pending")
		if err != nil {
			return err
		}
		committedID = run.ID
		_, err = CreateRunJob(ctx, tx, run.ID, "evaluate", "queued", []byte("{}"))
		return err
	})
	if commitErr != nil {
		t.Fatalf("commit transaction: %v", commitErr)
	}

	run, err = store.GetRun(ctx, committedID)
	if err != nil {
		t.Fatalf("expected committed run to be readable: %v", err)
	}
	if run.Status != "pending" {
		t.Fatalf("expected pending status, got %q", run.Status)
	}
}

func TestStateSnapshotCRUD(t *testing.T) {
	pool := newTestPool(t)
	store := NewStore(pool)
	ctx := context.Background()

	// Seed a scenario and run.
	scenario, err := store.CreateScenario(ctx, "snapshot-test", "Snapshot Test", "test scenario")
	if err != nil {
		t.Fatalf("create scenario: %v", err)
	}
	run, err := store.CreateRun(ctx, scenario.ID, "pending")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	// Create a "before" snapshot.
	before, err := CreateStateSnapshot(ctx, pool, run.ID, "step-1", "evt-001", "before", "order-100", []byte(`{"effect_count":0}`))
	if err != nil {
		t.Fatalf("create before snapshot: %v", err)
	}
	if before.SnapshotType != "before" {
		t.Fatalf("expected before snapshot type, got %q", before.SnapshotType)
	}
	if before.AggregateID != "order-100" {
		t.Fatalf("expected aggregate_id order-100, got %q", before.AggregateID)
	}

	// Create an "after" snapshot.
	after, err := CreateStateSnapshot(ctx, pool, run.ID, "step-1", "evt-001", "after", "order-100", []byte(`{"effect_count":1,"effects":[{"effect_type":"payment","amount_cents":1000}]}`))
	if err != nil {
		t.Fatalf("create after snapshot: %v", err)
	}
	if after.SnapshotType != "after" {
		t.Fatalf("expected after snapshot type, got %q", after.SnapshotType)
	}

	// List snapshots for the run.
	snaps, err := ListStateSnapshotsByRun(ctx, pool, run.ID)
	if err != nil {
		t.Fatalf("list state snapshots: %v", err)
	}
	if len(snaps) != 2 {
		t.Fatalf("expected 2 snapshots, got %d", len(snaps))
	}
	if snaps[0].SnapshotType != "before" || snaps[1].SnapshotType != "after" {
		t.Fatalf("expected before then after ordering, got %q and %q", snaps[0].SnapshotType, snaps[1].SnapshotType)
	}

	// Store method form.
	storeSnap, err := store.CreateStateSnapshot(ctx, run.ID, "step-2", "evt-002", "before", "sku-500", []byte(`{"effect_count":0}`))
	if err != nil {
		t.Fatalf("store CreateStateSnapshot: %v", err)
	}
	storeSnaps, err := store.ListStateSnapshotsByRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("store ListStateSnapshotsByRun: %v", err)
	}
	if len(storeSnaps) != 3 {
		t.Fatalf("expected 3 snapshots via store methods, got %d", len(storeSnaps))
	}
	_ = storeSnap
}
