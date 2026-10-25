// Package store provides SQL-first data access for GenesisGo run data.
//
// It is built on pgx/pgxpool connection pooling. Every table is scoped by
// run_id so that evidence from different runs can never be mixed.
package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/zannunakiz/GenesisGO/internal/observability"
)

// Store provides SQL-first access to GenesisGo run data. All public methods
// are safe for concurrent use because the underlying connection pool is.
type Store struct {
	pool *pgxpool.Pool
}

// NewStore wraps an existing connection pool. The caller retains ownership of
// the pool and must close it when done.
func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// Pool returns the underlying connection pool. It is exposed for callers that
// need raw pgx access (for example, custom transactions).
func (s *Store) Pool() *pgxpool.Pool {
	return s.pool
}

// Close releases the connection pool. It is safe to call Close multiple times.
func (s *Store) Close(ctx context.Context) {
	s.pool.Close()
	_ = ctx
}

// NewPool creates a connection pool from a PostgreSQL URL. pgxpool validates
// the configuration without requiring a live database; the first query
// establishes a connection. An empty URL is rejected immediately.
func NewPool(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	if databaseURL == "" {
		return nil, errors.New("database url is required")
	}
	poolConfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database pool config: %w", err)
	}
	poolConfig.ConnConfig.Tracer = observability.PGXQueryTracer{}
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("create postgres connection pool: %w", err)
	}
	return pool, nil
}

// CountQueuedRunJobs returns the current number of jobs waiting for a worker.
func (s *Store) CountQueuedRunJobs(ctx context.Context) (int64, error) {
	var count int64
	if err := s.pool.QueryRow(ctx, "SELECT count(*) FROM run_jobs WHERE status = 'queued'").Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}
