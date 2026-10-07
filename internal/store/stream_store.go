package store

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	"qcinspect/internal/statemachine"
)

// streamColumns is the canonical SELECT list scanned by scanStream (keep in
// sync with scanStream and the streams DDL).
const streamColumns = `id, name, normal_plan_id, tightened_plan_id, reduced_plan_id,
    production_stable, supervisor_approval,
    initial_production_stable, initial_supervisor_approval,
    current_state, version, created_at`

// CreateStreamParams creates a stream bound to three existing plans.
type CreateStreamParams struct {
	Name               string `json:"name"`
	NormalPlanID       int64  `json:"normal_plan_id"`
	TightenedPlanID    int64  `json:"tightened_plan_id"`
	ReducedPlanID      int64  `json:"reduced_plan_id"`
	ProductionStable   bool   `json:"production_stable"`
	SupervisorApproval bool   `json:"supervisor_approval"`
}

// CreateStream inserts a stream starting on normal inspection. Flags given
// here are the initial flags and apply from the beginning of the timeline;
// until the first PATCH with batches present there are no flag events.
func (s *Store) CreateStream(ctx context.Context, p CreateStreamParams) (StreamRow, error) {
	for _, pid := range []int64{p.NormalPlanID, p.TightenedPlanID, p.ReducedPlanID} {
		if _, err := s.GetPlan(ctx, pid); err != nil {
			return StreamRow{}, err
		}
	}
	state, _ := jsonMarshal(statemachine.InitialState())
	row := s.pool.QueryRow(ctx,
		`INSERT INTO streams
		    (name, normal_plan_id, tightened_plan_id, reduced_plan_id,
		     production_stable, supervisor_approval,
		     initial_production_stable, initial_supervisor_approval,
		     current_state)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		 RETURNING `+streamColumns,
		p.Name, p.NormalPlanID, p.TightenedPlanID, p.ReducedPlanID,
		p.ProductionStable, p.SupervisorApproval,
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
		`SELECT `+streamColumns+` FROM streams WHERE id=$1`, id)
	sr, err := scanStream(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return StreamRow{}, ErrNotFound
	}
	return sr, err
}

// ListStreams lists all streams.
func (s *Store) ListStreams(ctx context.Context) ([]StreamRow, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT `+streamColumns+` FROM streams ORDER BY name`)
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

func scanStream(r rowScanner) (StreamRow, error) {
	var sr StreamRow
	err := r.Scan(&sr.ID, &sr.Name, &sr.NormalPlanID, &sr.TightenedPlanID,
		&sr.ReducedPlanID, &sr.ProductionStable, &sr.SupervisorApproval,
		&sr.InitialStable, &sr.InitialApproval,
		&sr.CurrentState, &sr.Version, &sr.CreatedAt)
	return sr, err
}

type rowScannerAdapter struct{ r pgx.Rows }

func (a rowScannerAdapter) Scan(dest ...any) error { return a.r.Scan(dest...) }
