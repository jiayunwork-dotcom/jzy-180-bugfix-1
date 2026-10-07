package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"qcinspect/internal/replay"
	"qcinspect/internal/sampling"
	"qcinspect/internal/statemachine"
)

// checkpointEvery is the spacing between persisted checkpoints.
const checkpointEvery = 50

// lockStream returns the stream row with a row lock taken inside a
// transaction. All writes to one stream serialize here; writes to different
// streams take different row locks and never block each other.
func lockStream(ctx context.Context, tx pgx.Tx, id int64) (StreamRow, error) {
	row := tx.QueryRow(ctx,
		`SELECT `+streamColumns+` FROM streams WHERE id=$1 FOR UPDATE`, id)
	sr, err := scanStream(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return StreamRow{}, ErrNotFound
	}
	return sr, err
}

// boundPlans loads and validates the three plans bound to a stream.
func boundPlans(ctx context.Context, tx pgx.Tx, sr StreamRow) (statemachine.PlanTriple,
	map[statemachine.Severity]statemachine.PlanRef, error) {
	type sel struct {
		id  int64
		key statemachine.Severity
	}
	order := []sel{
		{sr.NormalPlanID, statemachine.Normal},
		{sr.TightenedPlanID, statemachine.Tightened},
		{sr.ReducedPlanID, statemachine.Reduced},
	}
	var triple statemachine.PlanTriple
	refs := map[statemachine.Severity]statemachine.PlanRef{}
	load := func(id int64) (PlanRow, sampling.Plan, error) {
		row := tx.QueryRow(ctx,
			`SELECT id, name, definition, created_at, updated_at FROM plans WHERE id=$1`, id)
		pr, err := scanPlan(row)
		if err != nil {
			return PlanRow{}, sampling.Plan{}, err
		}
		var pl sampling.Plan
		if err := json.Unmarshal(pr.Definition, &pl); err != nil {
			return PlanRow{}, sampling.Plan{}, fmt.Errorf("plan %d: %w", id, err)
		}
		if err := pl.Validate(); err != nil {
			return PlanRow{}, sampling.Plan{}, fmt.Errorf("plan %s: %w", pr.Name, err)
		}
		return pr, pl, nil
	}
	for _, o := range order {
		pr, pl, err := load(o.id)
		if err != nil {
			return statemachine.PlanTriple{}, nil, err
		}
		refs[o.key] = statemachine.PlanRef{PlanID: pr.ID, Name: pr.Name, Plan: pl}
		switch o.key {
		case statemachine.Normal:
			triple.Normal = pl
		case statemachine.Tightened:
			triple.Tightened = pl
		case statemachine.Reduced:
			triple.Reduced = pl
		}
	}
	return triple, refs, nil
}

// recomputeSuffix folds a stream's timeline from the best checkpoint at or
// before the suffix start (or from InitialState when fullReplay / no usable
// checkpoint), persists the folded suffix, rewrites checkpoints and updates
// the current state. Must be called inside a transaction after lockStream.
//
// startIdx is the 0-based index, in the NEW timeline, of the first item whose
// record may change. No checkpoint renumbering is needed: by construction the
// changed item sits at index startIdx, so exactly startIdx items precede it
// in both the old and new timelines; checkpoints at 1-based positions
// <= startIdx describe the same prefix states regardless of inserts, deletes
// or in-place edits. Checkpoints from position startIdx+1 on lie inside the
// suffix and are deleted and rebuilt.
func recomputeSuffix(ctx context.Context, tx pgx.Tx, sr StreamRow,
	startIdx int, fullReplay bool) (recomputeResult, error) {

	tlLoaded, err := loadTimeline(ctx, tx, sr.ID)
	if err != nil {
		return recomputeResult{}, err
	}
	triple, refs, err := boundPlans(ctx, tx, sr)
	if err != nil {
		return recomputeResult{}, err
	}
	items := buildOrderedItems(tlLoaded)
	if startIdx < 0 || startIdx > len(items) {
		startIdx = len(items)
	}

	state := statemachine.InitialState()
	startSeq := 0
	if !fullReplay {
		// Seed from the checkpoint at the immediately preceding 1-based
		// position (< startIdx+1, i.e. <= startIdx).
		cp, err := latestCheckpointAtOrBefore(ctx, tx, sr.ID, startIdx)
		if err != nil {
			return recomputeResult{}, err
		}
		if cp != nil && cp.itemSeq <= startIdx {
			state = cp.state
			startSeq = cp.itemSeq
		}
	}

	tl := replay.Timeline{Events: tlLoaded.resumes, FlagEvents: tlLoaded.flagEvents}
	byID := make(map[int64]loadedBatch, len(tlLoaded.batches))
	for _, b := range tlLoaded.batches {
		tl.Batches = append(tl.Batches, b.in)
		byID[b.row.ID] = b
	}

	initialFlags := replay.Flags{
		ProductionStable:   sr.InitialProductionStable,
		SupervisorApproval: sr.InitialSupervisorApproval,
	}
	// Replay seeds with the stream's INITIAL flags; flag events on the
	// timeline change them as the fold proceeds.
	entries, finalState, err := replay.FoldFrom(state, triple, refs, initialFlags, tl, startSeq)
	if err != nil {
		if errors.Is(err, statemachine.ErrSuspended) {
			return recomputeResult{}, ErrSuspended
		}
		return recomputeResult{}, err
	}
	// The flags in force at the end of the timeline are the last entry's
	// FlagsAfter (events included), or the initial flags for an empty
	// timeline. These — not the value just PATCHed — are the authoritative
	// "current flags": a change submitted after batches were deleted can end
	// up earlier on the timeline than an older change, and replay decides.
	finalFlags := initialFlags
	if len(entries) > 0 {
		finalFlags = entries[len(entries)-1].FlagsAfter
	}

	// Checkpoints at new positions inside the suffix are stale; drop them so
	// the fold can rewrite the periodic positions.
	if _, err := tx.Exec(ctx,
		`DELETE FROM stream_checkpoints WHERE stream_id=$1 AND item_seq >= $2`,
		sr.ID, startIdx+1); err != nil {
		return recomputeResult{}, err
	}

	out := recomputeResult{State: finalState, BatchByID: map[int64]statemachine.BatchResult{}}
	for _, e := range entries {
		if e.ItemSeq%checkpointEvery == 0 {
			if err := writeCheckpoint(ctx, tx, sr.ID, e.ItemSeq, e.Ordinal, e.StateAfter); err != nil {
				return recomputeResult{}, err
			}
		}
		if e.IsEvent {
			continue
		}
		raw, err := json.Marshal(e.Result)
		if err != nil {
			return recomputeResult{}, err
		}
		if _, err := tx.Exec(ctx,
			`UPDATE batches SET result=$2 WHERE id=$1`, e.BatchID, raw); err != nil {
			return recomputeResult{}, err
		}
		out.BatchByID[e.BatchID] = e.Result
		out.OrderedIDs = append(out.OrderedIDs, e.BatchID)
	}

	stateRaw, err := json.Marshal(finalState)
	if err != nil {
		return recomputeResult{}, err
	}
	if _, err := tx.Exec(ctx,
		`UPDATE streams
		 SET current_state=$2,
		     production_stable=$3, supervisor_approval=$4,
		     version=version+1
		 WHERE id=$1`,
		sr.ID, stateRaw, finalFlags.ProductionStable, finalFlags.SupervisorApproval); err != nil {
		return recomputeResult{}, err
	}
	return out, nil
}

// recomputeResult summarizes a suffix fold.
type recomputeResult struct {
	State      statemachine.State
	BatchByID  map[int64]statemachine.BatchResult
	OrderedIDs []int64
}
