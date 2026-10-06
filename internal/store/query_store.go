package store

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"
)

// ListBatches returns a stream's batches in chronological fold order with
// their folded records (severity, plan used, decision, switching score).
func (s *Store) ListBatches(ctx context.Context, streamID int64) ([]BatchView, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, batch_no, inspected_at, d1, d2, result
		 FROM batches WHERE stream_id=$1
		 ORDER BY inspected_at, id`, streamID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []BatchView
	for rows.Next() {
		bv, err := scanBatchView(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, bv)
	}
	return out, rows.Err()
}

// GetBatch fetches one batch.
func (s *Store) GetBatch(ctx context.Context, streamID, batchID int64) (BatchView, error) {
	row := s.pool.QueryRow(ctx,
		`SELECT id, batch_no, inspected_at, d1, d2, result
		 FROM batches WHERE stream_id=$1 AND id=$2`, streamID, batchID)
	bv, err := scanBatchView(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return BatchView{}, ErrNotFound
	}
	return bv, err
}

func scanBatchView(r rowScanner) (BatchView, error) {
	var bv BatchView
	var resultRaw []byte
	var d2 *int
	if err := r.Scan(&bv.ID, &bv.BatchNo, &bv.InspectedAt, &bv.D1, &d2, &resultRaw); err != nil {
		return BatchView{}, err
	}
	bv.D2 = d2
	var rec map[string]interface{}
	if len(resultRaw) > 0 {
		if err := json.Unmarshal(resultRaw, &rec); err != nil {
			return BatchView{}, err
		}
	}
	bv.Result = rec
	return bv, nil
}

// StreamDetail is a stream with its parsed current state and batch history.
type StreamDetail struct {
	Stream             StreamRow   `json:"stream"`
	State              interface{} `json:"state"`
	Batches            []BatchView `json:"batches"`
	ProductionStable   bool        `json:"production_stable"`
	SupervisorApproval bool        `json:"supervisor_approval"`
}

// GetStreamDetail loads a stream, parsed state and ordered batches.
func (s *Store) GetStreamDetail(ctx context.Context, id int64) (StreamDetail, error) {
	sr, err := s.GetStream(ctx, id)
	if err != nil {
		return StreamDetail{}, err
	}
	st, err := decodeState(sr)
	if err != nil {
		return StreamDetail{}, err
	}
	batches, err := s.ListBatches(ctx, id)
	if err != nil {
		return StreamDetail{}, err
	}
	return StreamDetail{
		Stream:             sr,
		State:              st,
		Batches:            batches,
		ProductionStable:   sr.ProductionStable,
		SupervisorApproval: sr.SupervisorApproval,
	}, nil
}
