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
		`SELECT `+streamColumns+`
		 FROM streams WHERE id=$1 FOR UPDATE`, id)
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
// before startIdx (or from InitialState when fullReplay / no checkpoint),
// persists the folded suffix, rewrites checkpoints and updates the current
// state. Must be called inside a transaction after lockStream.
//
// Checkpoints store machine state only; the control flags in effect at the
// suffix start are re-derived independently from the flag events before that
// position (and the stream's initial flags), so they can never be seeded from
// stale data.
func recomputeSuffix(ctx context.Context, tx pgx.Tx, sr StreamRow,
	startIdx int, fullReplay bool) (recomputeResult, error) {

	lt, err := loadTimeline(ctx, tx, sr.ID)
	if err != nil {
		return recomputeResult{}, err
	}
	triple, refs, err := boundPlans(ctx, tx, sr)
	if err != nil {
		return recomputeResult{}, err
	}
	items := buildOrderedItems(lt)
	if startIdx < 0 || startIdx > len(items) {
		startIdx = len(items)
	}

	state := statemachine.InitialState()
	startSeq := 0
	if !fullReplay {
		cp, err := latestCheckpointAtOrBefore(ctx, tx, sr.ID, startIdx)
		if err != nil {
			return recomputeResult{}, err
		}
		if cp != nil && cp.itemSeq <= startIdx {
			state = cp.state
			startSeq = cp.itemSeq
		}
	}
	initialFlags := replay.Flags{
		ProductionStable:   sr.InitialStable,
		SupervisorApproval: sr.InitialApproval,
	}
	// Flags in effect immediately before startSeq: walk only the ordered
	// events (cheap) instead of folding batches up to the cut.
	startFlags := initialFlags
	for _, it := range items[:minInt(startSeq, len(items))] {
		if it.isFlags {
			if f, ok := flagsValueAt(lt, it.eventID); ok {
				startFlags = f
			}
		}
	}

	tl := replay.Timeline{
		Batches:      batchInputs(lt),
		ResumeEvents: lt.resumes,
		FlagsEvents:  lt.flags,
		InitialFlags: initialFlags,
	}

	entries, finalState, err := replay.FoldFrom(state, triple, refs, startFlags, tl, startSeq)
	if err != nil {
		if errors.Is(err, statemachine.ErrSuspended) {
			return recomputeResult{}, ErrSuspended
		}
		return recomputeResult{}, err
	}

	// Old checkpoints inside the recomputed suffix are stale; drop them.
	if _, err := tx.Exec(ctx,
		`DELETE FROM stream_checkpoints WHERE stream_id=$1 AND item_seq >= $2`,
		sr.ID, startSeq+1); err != nil {
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
		`UPDATE streams SET current_state=$2, version=version+1 WHERE id=$1`,
		sr.ID, stateRaw); err != nil {
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

func batchInputs(lt loadedTimeline) []statemachine.BatchInput {
	out := make([]statemachine.BatchInput, 0, len(lt.batches))
	for _, b := range lt.batches {
		out = append(out, b.in)
	}
	return out
}

func flagsValueAt(lt loadedTimeline, eventID int64) (replay.Flags, bool) {
	for _, e := range lt.flags {
		if e.ID == eventID {
			return e.Flags, true
		}
	}
	return replay.Flags{}, false
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
