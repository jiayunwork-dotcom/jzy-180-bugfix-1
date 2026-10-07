package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"qcinspect/internal/statemachine"
)

// BatchInput is a request to add or correct a batch result.
type BatchInput struct {
	BatchNo     string    `json:"batch_no"`
	InspectedAt time.Time `json:"inspected_at"`
	D1          int       `json:"d1"`
	D2          *int      `json:"d2,omitempty"`
}

// BatchView is a batch returned to clients.
type BatchView struct {
	ID          int64                  `json:"id"`
	BatchNo     string                 `json:"batch_no"`
	InspectedAt time.Time              `json:"inspected_at"`
	D1          int                    `json:"d1"`
	D2          *int                   `json:"d2,omitempty"`
	Result      map[string]interface{} `json:"result"`
}

// MutationResult reports the new current state and per-batch records after a
// change, so clients can immediately see the recomputed history.
type MutationResult struct {
	State   statemachine.State  `json:"state"`
	Batches map[int64]BatchView `json:"batches,omitempty"`
	All     []BatchView         `json:"all_batches,omitempty"`
}

// AddBatch appends/backfills a batch and recomputes the affected suffix.
// Inserting while the stream is suspended is refused (a resume event must be
// recorded first).
func (s *Store) AddBatch(ctx context.Context, streamID int64, in BatchInput) (MutationResult, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return MutationResult{}, err
	}
	defer tx.Rollback(ctx)

	sr, err := lockStream(ctx, tx, streamID)
	if err != nil {
		return MutationResult{}, err
	}
	if err := validateBatchAgainstPlans(ctx, tx, sr, in); err != nil {
		return MutationResult{}, err
	}

	// Determine earliest affected index under the NEW timeline (after insert).
	startIdx, err := earliestAffectedForInsert(ctx, tx, streamID, in.InspectedAt)
	if err != nil {
		return MutationResult{}, err
	}

	var id int64
	err = tx.QueryRow(ctx,
		`INSERT INTO batches (stream_id, batch_no, inspected_at, d1, d2, result)
		 VALUES ($1,$2,$3,$4,$5,'{}'::jsonb) RETURNING id`,
		streamID, in.BatchNo, in.InspectedAt.UTC(), in.D1, in.D2).Scan(&id)
	if err != nil {
		return MutationResult{}, mapWriteError(err)
	}

	rec, err := recomputeSuffix(ctx, tx, sr, startIdx, false)
	if err != nil {
		return MutationResult{}, err
	}
	return s.finishMutation(ctx, tx, streamID, rec)
}

// UpdateBatch corrects a batch (count or timestamp) and recomputes.
func (s *Store) UpdateBatch(ctx context.Context, streamID, batchID int64, in BatchInput) (MutationResult, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return MutationResult{}, err
	}
	defer tx.Rollback(ctx)

	sr, err := lockStream(ctx, tx, streamID)
	if err != nil {
		return MutationResult{}, err
	}
	var oldAt time.Time
	var oldStream int64
	err = tx.QueryRow(ctx,
		`SELECT stream_id, inspected_at FROM batches WHERE id=$1`, batchID).
		Scan(&oldStream, &oldAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return MutationResult{}, ErrNotFound
	}
	if err != nil {
		return MutationResult{}, err
	}
	if oldStream != streamID {
		return MutationResult{}, ErrNotFound
	}
	if err := validateBatchAgainstPlans(ctx, tx, sr, in); err != nil {
		return MutationResult{}, err
	}
	// Earliest position between old and new timestamp.
	minT := oldAt
	if in.InspectedAt.Before(minT) {
		minT = in.InspectedAt
	}
	lt, err := loadTimeline(ctx, tx, streamID)
	if err != nil {
		return MutationResult{}, err
	}
	items := buildOrderedItems(lt)
	startIdx := firstIndexFromTime(items, minT)

	ct, err := tx.Exec(ctx,
		`UPDATE batches SET batch_no=$2, inspected_at=$3, d1=$4, d2=$5 WHERE id=$1`,
		batchID, in.BatchNo, in.InspectedAt.UTC(), in.D1, in.D2)
	if err != nil {
		return MutationResult{}, mapWriteError(err)
	}
	if ct.RowsAffected() == 0 {
		return MutationResult{}, ErrNotFound
	}

	rec, err := recomputeSuffix(ctx, tx, sr, startIdx, false)
	if err != nil {
		return MutationResult{}, err
	}
	return s.finishMutation(ctx, tx, streamID, rec)
}

// DeleteBatch removes a batch and recomputes from its position onward.
func (s *Store) DeleteBatch(ctx context.Context, streamID, batchID int64) (MutationResult, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return MutationResult{}, err
	}
	defer tx.Rollback(ctx)

	sr, err := lockStream(ctx, tx, streamID)
	if err != nil {
		return MutationResult{}, err
	}
	var at time.Time
	var oldStream int64
	err = tx.QueryRow(ctx,
		`SELECT stream_id, inspected_at FROM batches WHERE id=$1`, batchID).
		Scan(&oldStream, &at)
	if errors.Is(err, pgx.ErrNoRows) {
		return MutationResult{}, ErrNotFound
	}
	if err != nil {
		return MutationResult{}, err
	}
	if oldStream != streamID {
		return MutationResult{}, ErrNotFound
	}
	lt, err := loadTimeline(ctx, tx, streamID)
	if err != nil {
		return MutationResult{}, err
	}
	items := buildOrderedItems(lt)
	startIdx := firstIndexFromTime(items, at)

	if _, err := tx.Exec(ctx, `DELETE FROM batches WHERE id=$1`, batchID); err != nil {
		return MutationResult{}, err
	}
	rec, err := recomputeSuffix(ctx, tx, sr, startIdx, false)
	if err != nil {
		return MutationResult{}, err
	}
	return s.finishMutation(ctx, tx, streamID, rec)
}

// Resume records a manual resume after suspension. Inspection restarts under
// tightened rules; later batches fold from a fresh tightened state at the
// resume point.
func (s *Store) Resume(ctx context.Context, streamID int64, at time.Time) (MutationResult, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return MutationResult{}, err
	}
	defer tx.Rollback(ctx)

	sr, err := lockStream(ctx, tx, streamID)
	if err != nil {
		return MutationResult{}, err
	}
	lt, err := loadTimeline(ctx, tx, streamID)
	if err != nil {
		return MutationResult{}, err
	}
	items := buildOrderedItems(lt)
	startIdx := firstIndexFromTime(items, at)

	var eventID int64
	err = tx.QueryRow(ctx,
		`INSERT INTO stream_events (stream_id, kind, occurred_at)
		 VALUES ($1,'resume',$2) RETURNING id`, streamID, at.UTC()).Scan(&eventID)
	if err != nil {
		return MutationResult{}, err
	}
	rec, err := recomputeSuffix(ctx, tx, sr, startIdx, false)
	if err != nil {
		return MutationResult{}, err
	}
	_ = eventID
	return s.finishMutation(ctx, tx, streamID, rec)
}

// SetFlags changes the stream control flags ("production stable",
// "supervisor approval").
//
// The change is scoped by a FIXED boundary: the batch with the latest
// inspected_at present on the stream at commit time. Everything up to and
// including that boundary batch keeps being judged under the old flags; only
// batches positioned strictly after it see the new values. Persisted records
// of earlier batches therefore never change because a flag was toggled.
//
//   - With at least one batch on the stream, a 'flags' event is written at
//     the boundary timestamp (ordered after every same-instant batch) and the
//     suffix after it is folded. The event's position is frozen: later
//     backfills/deletes do not move it, which is exactly what makes
//     "replay the whole history in timeline order" bit-for-bit identical to
//     what the stream lived through.
//   - With no batches yet, there is no boundary, so the change applies from
//     the beginning: any earlier flag events are discarded and the stream's
//     initial flags are rewritten.
//
// nil pointers leave the respective flag unchanged; merging happens on the
// locked stream row, so two concurrent PATCHes cannot lose each other's
// values.
func (s *Store) SetFlags(ctx context.Context, streamID int64,
	productionStable, supervisorApproval *bool) (MutationResult, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return MutationResult{}, err
	}
	defer tx.Rollback(ctx)
	sr, err := lockStream(ctx, tx, streamID)
	if err != nil {
		return MutationResult{}, err
	}
	newStable, newApproval := sr.ProductionStable, sr.SupervisorApproval
	if productionStable != nil {
		newStable = *productionStable
	}
	if supervisorApproval != nil {
		newApproval = *supervisorApproval
	}
	// Nothing actually changed: no event, no recompute. (A no-op PATCH must
	// not create a zero-effect event that would shift item seqs.)
	if newStable == sr.ProductionStable && newApproval == sr.SupervisorApproval {
		cur, err := decodeState(sr)
		if err != nil {
			return MutationResult{}, err
		}
		return s.finishMutation(ctx, tx, streamID, recomputeResult{State: cur})
	}

	var boundary *time.Time
	// max inspected_at over existing batches = the boundary.
	if err := tx.QueryRow(ctx,
		`SELECT max(inspected_at) FROM batches WHERE stream_id=$1`, streamID).
		Scan(&boundary); err != nil {
		return MutationResult{}, err
	}
	hasBatches := boundary != nil

	if !hasBatches {
		// No boundary exists: from the beginning. Drop prior flag events
		// (they can only have come from PATCHes also made without batches)
		// and rewrite the initial flags.
		if _, err := tx.Exec(ctx,
			`DELETE FROM stream_events WHERE stream_id=$1 AND kind='flags'`, streamID); err != nil {
			return MutationResult{}, err
		}
		if _, err := tx.Exec(ctx,
			`UPDATE streams
			 SET production_stable=$2, supervisor_approval=$3,
			     initial_production_stable=$2, initial_supervisor_approval=$3
			 WHERE id=$1`,
			streamID, newStable, newApproval); err != nil {
			return MutationResult{}, err
		}
		sr.ProductionStable = newStable
		sr.SupervisorApproval = newApproval
		sr.InitialStable = newStable
		sr.InitialApproval = newApproval
		rec, err := recomputeSuffix(ctx, tx, sr, 0, true)
		if err != nil {
			return MutationResult{}, err
		}
		return s.finishMutation(ctx, tx, streamID, rec)
	}

	// Insert the flags event at the boundary timestamp and find its index in
	// the new ordering. The event sorts after all batches at that instant.
	var eventID int64
	if err := tx.QueryRow(ctx,
		`INSERT INTO stream_events
		    (stream_id, kind, occurred_at, production_stable, supervisor_approval)
		 VALUES ($1,'flags',$2,$3,$4) RETURNING id`,
		streamID, (*boundary).UTC(), newStable, newApproval).Scan(&eventID); err != nil {
		return MutationResult{}, err
	}
	if _, err := tx.Exec(ctx,
		`UPDATE streams SET production_stable=$2, supervisor_approval=$3 WHERE id=$1`,
		streamID, newStable, newApproval); err != nil {
		return MutationResult{}, err
	}
	sr.ProductionStable = newStable
	sr.SupervisorApproval = newApproval

	lt, err := loadTimeline(ctx, tx, streamID)
	if err != nil {
		return MutationResult{}, err
	}
	items := buildOrderedItems(lt)
	startIdx := eventIndexByID(items, eventID)
	if startIdx < 0 {
		return MutationResult{}, errors.New("inserted flags event missing from timeline")
	}
	rec, err := recomputeSuffix(ctx, tx, sr, startIdx, false)
	if err != nil {
		return MutationResult{}, err
	}
	return s.finishMutation(ctx, tx, streamID, rec)
}

// earliestAffectedForInsert computes the suffix start as the index of the
// first existing item at or after the new batch's time.
func earliestAffectedForInsert(ctx context.Context, tx pgx.Tx, streamID int64, at time.Time) (int, error) {
	lt, err := loadTimeline(ctx, tx, streamID)
	if err != nil {
		return 0, err
	}
	items := buildOrderedItems(lt)
	return firstIndexFromTime(items, at), nil
}

func firstIndexFromTime(items []timelineItem, t time.Time) int {
	for i, it := range items {
		if !it.at.Before(t) {
			return i
		}
	}
	return len(items)
}

// eventIndexByID returns the 0-based timeline index of an event.
func eventIndexByID(items []timelineItem, eventID int64) int {
	for i, it := range items {
		if it.isEvent && it.eventID == eventID {
			return i
		}
	}
	return -1
}

func (s *Store) finishMutation(ctx context.Context, tx pgx.Tx, streamID int64,
	rec recomputeResult) (MutationResult, error) {
	rows, err := listBatchesTx(ctx, tx, streamID)
	if err != nil {
		return MutationResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return MutationResult{}, err
	}
	return MutationResult{State: rec.State, All: rows}, nil
}
