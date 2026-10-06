package store

import (
	"context"
	"encoding/json"
)

// UpdatePlanAndReplay updates a plan definition and replays every stream that
// binds it, since historical judgments are expressed against the current
// bound plans (documented consistency rule).
func (s *Store) UpdatePlanAndReplay(ctx context.Context, id int64, definition any) (PlanRow, []int64, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return PlanRow{}, nil, err
	}
	defer tx.Rollback(ctx)

	raw, err := json.Marshal(definition)
	if err != nil {
		return PlanRow{}, nil, err
	}
	row := tx.QueryRow(ctx,
		`UPDATE plans SET definition=$2, updated_at=now() WHERE id=$1
		 RETURNING id, name, definition, created_at, updated_at`, id, raw)
	pr, err := scanPlan(row)
	if err != nil {
		return PlanRow{}, nil, err
	}

	streamRows, err := tx.Query(ctx,
		`SELECT id FROM streams
		 WHERE normal_plan_id=$1 OR tightened_plan_id=$1 OR reduced_plan_id=$1
		 ORDER BY id`, id)
	if err != nil {
		return PlanRow{}, nil, err
	}
	var ids []int64
	for streamRows.Next() {
		var sid int64
		if err := streamRows.Scan(&sid); err != nil {
			streamRows.Close()
			return PlanRow{}, nil, err
		}
		ids = append(ids, sid)
	}
	streamRows.Close()
	if err := streamRows.Err(); err != nil {
		return PlanRow{}, nil, err
	}

	for _, sid := range ids {
		sr, err := lockStream(ctx, tx, sid)
		if err != nil {
			return PlanRow{}, nil, err
		}
		if _, err := recomputeSuffix(ctx, tx, sr, 0, true); err != nil {
			return PlanRow{}, nil, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return PlanRow{}, nil, err
	}
	return pr, ids, nil
}
