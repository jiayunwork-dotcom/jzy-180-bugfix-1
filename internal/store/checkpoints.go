package store

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"

	"qcinspect/internal/statemachine"
)

type checkpointRow struct {
	itemSeq int
	ordinal int
	state   statemachine.State
}

// latestCheckpointAtOrBefore returns the most recent checkpoint whose
// item_seq <= seq, or nil when there is none.
func latestCheckpointAtOrBefore(ctx context.Context, tx pgx.Tx, streamID int64, seq int) (*checkpointRow, error) {
	if seq <= 0 {
		return nil, nil
	}
	row := tx.QueryRow(ctx,
		`SELECT item_seq, ordinal, state FROM stream_checkpoints
		 WHERE stream_id=$1 AND item_seq <= $2
		 ORDER BY item_seq DESC LIMIT 1`, streamID, seq)
	var cp checkpointRow
	var raw []byte
	if err := row.Scan(&cp.itemSeq, &cp.ordinal, &raw); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	if err := json.Unmarshal(raw, &cp.state); err != nil {
		return nil, err
	}
	return &cp, nil
}

func writeCheckpoint(ctx context.Context, tx pgx.Tx, streamID int64,
	itemSeq, ordinal int, st statemachine.State) error {
	raw, err := json.Marshal(st)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx,
		`INSERT INTO stream_checkpoints (stream_id, item_seq, ordinal, state)
		 VALUES ($1,$2,$3,$4)
		 ON CONFLICT (stream_id, item_seq) DO UPDATE SET ordinal=EXCLUDED.ordinal, state=EXCLUDED.state`,
		streamID, itemSeq, ordinal, raw)
	return err
}
