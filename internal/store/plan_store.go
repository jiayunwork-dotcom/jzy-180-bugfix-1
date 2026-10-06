package store

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// CreatePlan inserts a plan. Name collisions return ErrDuplicateName.
func (s *Store) CreatePlan(ctx context.Context, name string, definition any) (PlanRow, error) {
	raw, err := json.Marshal(definition)
	if err != nil {
		return PlanRow{}, err
	}
	row := s.pool.QueryRow(ctx,
		`INSERT INTO plans (name, definition) VALUES ($1, $2)
		 RETURNING id, name, definition, created_at, updated_at`, name, raw)
	pr, err := scanPlan(row)
	if err != nil {
		return PlanRow{}, mapWriteError(err)
	}
	return pr, nil
}

// GetPlan loads a plan by id.
func (s *Store) GetPlan(ctx context.Context, id int64) (PlanRow, error) {
	row := s.pool.QueryRow(ctx,
		`SELECT id, name, definition, created_at, updated_at FROM plans WHERE id=$1`, id)
	pr, err := scanPlan(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return PlanRow{}, ErrNotFound
	}
	return pr, err
}

// GetPlanByName loads a plan by name.
func (s *Store) GetPlanByName(ctx context.Context, name string) (PlanRow, error) {
	row := s.pool.QueryRow(ctx,
		`SELECT id, name, definition, created_at, updated_at FROM plans WHERE name=$1`, name)
	pr, err := scanPlan(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return PlanRow{}, ErrNotFound
	}
	return pr, err
}

// ListPlans lists all plans ordered by name.
func (s *Store) ListPlans(ctx context.Context) ([]PlanRow, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, name, definition, created_at, updated_at FROM plans ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PlanRow
	for rows.Next() {
		pr, err := scanPlan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, pr)
	}
	return out, rows.Err()
}

// UpdatePlan overwrites a plan definition.
func (s *Store) UpdatePlan(ctx context.Context, id int64, definition any) (PlanRow, error) {
	raw, err := json.Marshal(definition)
	if err != nil {
		return PlanRow{}, err
	}
	row := s.pool.QueryRow(ctx,
		`UPDATE plans SET definition=$2, updated_at=now() WHERE id=$1
		 RETURNING id, name, definition, created_at, updated_at`, id, raw)
	pr, err := scanPlan(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return PlanRow{}, ErrNotFound
	}
	return pr, err
}

// DeletePlan removes a plan. It refuses with ErrBoundPlan while any stream
// references it (bindings are part of history and must not dangle).
func (s *Store) DeletePlan(ctx context.Context, id int64) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var n int
	if err := tx.QueryRow(ctx,
		`SELECT count(*) FROM streams
		 WHERE normal_plan_id=$1 OR tightened_plan_id=$1 OR reduced_plan_id=$1`, id).Scan(&n); err != nil {
		return err
	}
	if n > 0 {
		return ErrBoundPlan
	}
	ct, err := tx.Exec(ctx, `DELETE FROM plans WHERE id=$1`, id)
	if err != nil {
		return err
	}
	if ct.RowsAffected() == 0 {
		return ErrNotFound
	}
	return tx.Commit(ctx)
}

// StreamIDsUsingPlan reports which streams currently bind a plan.
func (s *Store) StreamIDsUsingPlan(ctx context.Context, id int64) ([]int64, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id FROM streams
		 WHERE normal_plan_id=$1 OR tightened_plan_id=$1 OR reduced_plan_id=$1
		 ORDER BY id`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var id2 int64
		if err := rows.Scan(&id2); err != nil {
			return nil, err
		}
		out = append(out, id2)
	}
	return out, rows.Err()
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanPlan(r rowScanner) (PlanRow, error) {
	var pr PlanRow
	err := r.Scan(&pr.ID, &pr.Name, &pr.Definition, &pr.CreatedAt, &pr.UpdatedAt)
	return pr, err
}

func mapWriteError(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return ErrDuplicateName
	}
	return err
}
