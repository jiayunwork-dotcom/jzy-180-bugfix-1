package store

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"

	"qcinspect/internal/statemachine"
)

// CreateStreamParams creates a stream bound to three existing plans.
type CreateStreamParams struct {
	Name               string `json:"name"`
	NormalPlanID       int64  `json:"normal_plan_id"`
	TightenedPlanID    int64  `json:"tightened_plan_id"`
	ReducedPlanID      int64  `json:"reduced_plan_id"`
	ProductionStable   bool   `json:"production_stable"`
	SupervisorApproval bool   `json:"supervisor_approval"`
}

// CreateStream inserts a stream starting on normal inspection.
func (s *Store) CreateStream(ctx context.Context, p CreateStreamParams) (StreamRow, error) {
	for _, pid := range []int64{p.NormalPlanID, p.TightenedPlanID, p.ReducedPlanID} {
		if _, err := s.GetPlan(ctx, pid); err != nil {
			return StreamRow{}, err
		}
	}
	state, _ := json.Marshal(statemachine.InitialState())
	row := s.pool.QueryRow(ctx,
		`INSERT INTO streams
		    (name, normal_plan_id, tightened_plan_id, reduced_plan_id,
		     production_stable, supervisor_approval, current_state)
		 VALUES ($1,$2,$3,$4,$5,$6,$7)
		 RETURNING id, name, normal_plan_id, tightened_plan_id, reduced_plan_id,
		           production_stable, supervisor_approval, current_state, version, created_at`,
		p.Name, p.NormalPlanID, p.TightenedPlanID, p.ReducedPlanID,
		p.ProductionStable, p.SupervisorApproval, state)
	sr, err := scanStream(row)
	if err != nil {
		return StreamRow{}, mapWriteError(err)
	}
	return sr, nil
}

// GetStream loads a stream.
func (s *Store) GetStream(ctx context.Context, id int64) (StreamRow, error) {
	row := s.pool.QueryRow(ctx,
		`SELECT id, name, normal_plan_id, tightened_plan_id, reduced_plan_id,
		        production_stable, supervisor_approval, current_state, version, created_at
		 FROM streams WHERE id=$1`, id)
	sr, err := scanStream(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return StreamRow{}, ErrNotFound
	}
	return sr, err
}

// ListStreams lists all streams.
func (s *Store) ListStreams(ctx context.Context) ([]StreamRow, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, name, normal_plan_id, tightened_plan_id, reduced_plan_id,
		        production_stable, supervisor_approval, current_state, version, created_at
		 FROM streams ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []StreamRow
	for rows.Next() {
		sr, err := scanStream(rowScannerAdapter{rows})
		if err != nil {
			return nil, err
		}
		out = append(out, sr)
	}
	return out, rows.Err()
}

// StreamFlagUpdate changes the control flags (full replay follows).
type StreamFlagUpdate struct {
	ProductionStable   *bool
	SupervisorApproval *bool
}

// UpdateStreamFlags updates the stream flags and returns the new row.
func (s *Store) UpdateStreamFlags(ctx context.Context, id int64, u StreamFlagUpdate) (StreamRow, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return StreamRow{}, err
	}
	defer tx.Rollback(ctx)
	sr, err := lockStream(ctx, tx, id)
	if err != nil {
		return StreamRow{}, err
	}
	if u.ProductionStable != nil {
		sr.ProductionStable = *u.ProductionStable
	}
	if u.SupervisorApproval != nil {
		sr.SupervisorApproval = *u.SupervisorApproval
	}
	row := tx.QueryRow(ctx,
		`UPDATE streams SET production_stable=$2, supervisor_approval=$3 WHERE id=$1
		 RETURNING id, name, normal_plan_id, tightened_plan_id, reduced_plan_id,
		           production_stable, supervisor_approval, current_state, version, created_at`,
		id, sr.ProductionStable, sr.SupervisorApproval)
	sr, err = scanStream(row)
	if err != nil {
		return StreamRow{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return StreamRow{}, err
	}
	return sr, nil
}

func scanStream(r rowScanner) (StreamRow, error) {
	var sr StreamRow
	err := r.Scan(&sr.ID, &sr.Name, &sr.NormalPlanID, &sr.TightenedPlanID,
		&sr.ReducedPlanID, &sr.ProductionStable, &sr.SupervisorApproval,
		&sr.CurrentState, &sr.Version, &sr.CreatedAt)
	return sr, err
}

type rowScannerAdapter struct{ r pgx.Rows }

func (a rowScannerAdapter) Scan(dest ...any) error { return a.r.Scan(dest...) }
