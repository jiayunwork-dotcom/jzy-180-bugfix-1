// Package store implements PostgreSQL persistence for plans, streams,
// batches, administrative events and replay checkpoints.
package store

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Store is the persistence facade.
type Store struct {
	pool *pgxpool.Pool
}

// New connects the pool and ensures the schema exists.
func New(ctx context.Context, url string) (*Store, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	s := &Store{pool: pool}
	if err := s.migrate(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) migrate(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, migrationSQL)
	return err
}

// Pool exposes the underlying pool for tests.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// Close releases the pool.
func (s *Store) Close() { s.pool.Close() }

// Sentinel errors mapped by the HTTP layer.
var (
	ErrNotFound      = errors.New("not found")
	ErrConflict      = errors.New("conflict")
	ErrSuspended     = errors.New("stream suspended")
	ErrBoundPlan     = errors.New("plan is bound to streams")
	ErrDuplicateName = errors.New("duplicate name")
)
