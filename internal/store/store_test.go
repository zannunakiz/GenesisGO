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
