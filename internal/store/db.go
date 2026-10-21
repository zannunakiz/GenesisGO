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
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("create postgres connection pool: %w", err)
	}
	return pool, nil
}
