package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

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
	if err := tx.Commit(ctx); err != nil {
		return MutationResult{}, err
	}
	return s.mutationView(ctx, streamID, rec, id)
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
	tl, err := loadTimeline(ctx, tx, streamID)
	if err != nil {
		return MutationResult{}, err
	}
	items := buildOrderedItems(tl)
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
	if err := tx.Commit(ctx); err != nil {
		return MutationResult{}, err
	}
	return s.mutationView(ctx, streamID, rec, batchID)
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
	tl, err := loadTimeline(ctx, tx, streamID)
	if err != nil {
		return MutationResult{}, err
	}
	items := buildOrderedItems(tl)
	startIdx := firstIndexFromTime(items, at)

	if _, err := tx.Exec(ctx, `DELETE FROM batches WHERE id=$1`, batchID); err != nil {
		return MutationResult{}, err
	}
	rec, err := recomputeSuffix(ctx, tx, sr, startIdx, false)
	if err != nil {
		return MutationResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return MutationResult{}, err
	}
	return s.mutationView(ctx, streamID, rec, 0)
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
	tl, err := loadTimeline(ctx, tx, streamID)
	if err != nil {
		return MutationResult{}, err
	}
	items := buildOrderedItems(tl)
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
	if err := tx.Commit(ctx); err != nil {
		return MutationResult{}, err
	}
	_ = eventID
	return s.mutationView(ctx, streamID, rec, 0)
}

// SetFlags records a point-in-time change of the control flags.
//
// The change only governs batches later on the timeline. Its effective point
// is the latest batch inspection time present on the stream at commit time:
// a flags event is inserted exactly at that boundary, so every existing batch
// keeps the record it already had and only batches entered afterwards are
// judged with the new flags. A batch later backfilled at a time at or before
// the boundary is still judged with the flags in force then (batches precede
// flag events at equal times).
//
// When the stream has no batches yet there is no history to protect: no event
// is recorded, the stream's initial flags are rewritten and the change
// applies from the timeline start ("从头算起"); any earlier flag events (left
// over after batches were deleted) are discarded.
//
// Flag events are immutable: they are never moved when batches are later
// backfilled or deleted, and recomputeSuffix writes back the flags produced
// by replaying the full timeline. Consequently, after an edit sequence such
// as "change flags, delete all batches of that period, backfill earlier lots,
// change flags again", the stream's reported current flags are the last
// flags on the replay timeline, which need not equal this call's arguments.
func (s *Store) SetFlags(ctx context.Context, streamID int64,
	productionStable, supervisorApproval bool) (MutationResult, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return MutationResult{}, err
	}
	defer tx.Rollback(ctx)
	sr, err := lockStream(ctx, tx, streamID)
	if err != nil {
		return MutationResult{}, err
	}

	var latestAt *time.Time
	if err := tx.QueryRow(ctx,
		`SELECT max(inspected_at) FROM batches WHERE stream_id=$1`, streamID).
		Scan(&latestAt); err != nil {
		return MutationResult{}, err
	}
	if latestAt == nil {
		// No history: the change is the stream's baseline, from the start.
		// Earlier flag events only had meaning relative to batches that no
		// longer exist; drop them so the (now empty) timeline stays coherent.
		if _, err := tx.Exec(ctx,
			`DELETE FROM stream_events WHERE stream_id=$1 AND kind='flags'`, streamID); err != nil {
			return MutationResult{}, err
		}
		if _, err := tx.Exec(ctx,
			`UPDATE streams
			 SET production_stable=$2, supervisor_approval=$3,
			     initial_production_stable=$2, initial_supervisor_approval=$3
			 WHERE id=$1`,
			streamID, productionStable, supervisorApproval); err != nil {
			return MutationResult{}, err
		}
		sr.ProductionStable = productionStable
		sr.SupervisorApproval = supervisorApproval
		sr.InitialProductionStable = productionStable
		sr.InitialSupervisorApproval = supervisorApproval
		rec, err := recomputeSuffix(ctx, tx, sr, 0, true)
		if err != nil {
			return MutationResult{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return MutationResult{}, err
		}
		return s.mutationView(ctx, streamID, rec, 0)
	}

	tl, err := loadTimeline(ctx, tx, streamID)
	if err != nil {
		return MutationResult{}, err
	}
	oldItems := buildOrderedItems(tl)

	// The flags event is placed exactly at the boundary: the latest batch's
	// inspection time. Batches at that instant always precede it, so the
	// boundary lot keeps the record it already had. When flags are changed
	// several times without newer batches the events share that instant and
	// order by ascending id, i.e. the most recent submission is last and
	// carries the flags in force immediately after the boundary.
	boundary := latestAt.UTC()

	// Suffix start (0-based index in the NEW timeline): the new event's
	// position. Everything at or before the boundary that precedes it (all
	// earlier items, batches and resume events at the boundary) is unaffected;
	// only items after the event are folded with the new flags.
	startIdx := 0
	for _, it := range oldItems {
		if it.at.After(boundary) {
			break
		}
		startIdx++
	}

	if _, err := tx.Exec(ctx,
		`INSERT INTO stream_events
		    (stream_id, kind, occurred_at, production_stable, supervisor_approval)
		 VALUES ($1,'flags',$2,$3,$4)`,
		streamID, boundary, productionStable, supervisorApproval); err != nil {
		return MutationResult{}, err
	}

	rec, err := recomputeSuffix(ctx, tx, sr, startIdx, false)
	if err != nil {
		return MutationResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return MutationResult{}, err
	}
	return s.mutationView(ctx, streamID, rec, 0)
}

// earliestAffectedForInsert computes the suffix start as the index of the
// first existing item at or after the new batch's time.
func earliestAffectedForInsert(ctx context.Context, tx pgx.Tx, streamID int64, at time.Time) (int, error) {
	tl, err := loadTimeline(ctx, tx, streamID)
	if err != nil {
		return 0, err
	}
	items := buildOrderedItems(tl)
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

func (s *Store) mutationView(ctx context.Context, streamID int64,
	rec recomputeResult, focus int64) (MutationResult, error) {
	rows, err := s.ListBatches(ctx, streamID)
	if err != nil {
		return MutationResult{}, err
	}
	return MutationResult{State: rec.State, All: rows}, nil
}

// mapWriteError maps unique-violation errors to a sentinel.
func init() {
	_ = (*pgconn.PgError)(nil)
}
