// Package replay folds the complete history of a stream (batches plus
// administrative events) into per-batch records and the current state.
//
// Strategy: incremental recompute from a checkpoint, NOT full replay on every
// edit and NOT purely incremental state patching. The fold itself lives in
// statemachine.StepWithFlags and is a pure function, so recomputing a suffix
// from a persisted checkpoint state is exactly the same computation as folding
// the whole history from InitialState; checkpoints are only an optimization.
//
// Why this trade-off (see delivery notes):
//   - Full replay on every keystroke edit is O(history) and gets expensive on
//     long streams.
//   - Pure incremental delta-patching ("adjust the counters around the
//     changed batch") is exactly where score resets, the 5-batch windows and
//     suspension get silently missed; it duplicates the transition logic and
//     inevitably drifts from it.
//
// Instead the single source of truth (the pure fold) is re-run only on the
// suffix starting at the earliest affected position, seeded from a checkpoint
// taken strictly before that position. Worst case (edit of the earliest
// batch) is full replay; appends are O(1) plus a periodic checkpoint write.
package replay

import (
	"sort"

	"qcinspect/internal/statemachine"
)

// ResumeEvent is an administrative "resume after suspension" at a point in
// time. Folding one forces the stream into tightened inspection with all
// counters restarted (GB/T 2828.1: inspection restarts under tightened rules
// after authority to resume is granted).
type ResumeEvent struct {
	ID         int64
	OccurredAt string // RFC3339 timestamp
}

// Flags are stream-wide control inputs.
type Flags struct {
	ProductionStable   bool
	SupervisorApproval bool
}

// item is the unified timeline element.
type item struct {
	at      string
	batchID int64
	eventID int64
	event   bool
}

// Entry is one folded timeline element in output order.
type Entry struct {
	ItemSeq int // 1-based position across batches and events
	Ordinal int // 1-based batch ordinal (events do not advance it)
	BatchID int64
	EventID int64
	IsEvent bool
	Result  statemachine.BatchResult
	// StateAfter is the complete state immediately after this element; it is
	// what a checkpoint at ItemSeq stores.
	StateAfter statemachine.State
}

// Timeline orders batches and resume events for folding.
type Timeline struct {
	Batches []statemachine.BatchInput
	Events  []ResumeEvent
}

func (tl Timeline) items() []item {
	out := make([]item, 0, len(tl.Batches)+len(tl.Events))
	for _, b := range tl.Batches {
		out = append(out, item{at: b.InspectedAt, batchID: b.ID})
	}
	for _, e := range tl.Events {
		out = append(out, item{at: e.OccurredAt, eventID: e.ID, event: true})
	}
	// Chronological order; ties broken by batches-before-events at the same
	// instant, then by source id, which keeps ordering deterministic under
	// backdated inserts.
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].at != out[j].at {
			return out[i].at < out[j].at
		}
		if out[i].event != out[j].event {
			return !out[i].event
		}
		if out[i].batchID != out[j].batchID {
			return out[i].batchID < out[j].batchID
		}
		return out[i].eventID < out[j].eventID
	})
	return out
}

// fold folds timeline items starting from item index startSeq (0-based; items
// before it are skipped and must match the checkpoint that produced start).
// Returned entries cover the suffix; finalOrdinal is the batch ordinal at the
// end (needed to keep checkpoints' ordinal field correct).
func fold(start statemachine.State, plans statemachine.PlanTriple,
	refs map[statemachine.Severity]statemachine.PlanRef, fl Flags,
	tl Timeline, startSeq int) ([]Entry, statemachine.State, error) {

	items := tl.items()
	payload := make(map[int64]statemachine.BatchInput, len(tl.Batches))
	for _, b := range tl.Batches {
		payload[b.ID] = b
	}
	smFlags := statemachine.FlagsExported{
		ProductionStable:   fl.ProductionStable,
		SupervisorApproval: fl.SupervisorApproval,
	}
	st := start
	ordinal := st.Ordinal
	if startSeq < 0 {
		startSeq = 0
	}
	entries := make([]Entry, 0, len(items)-startSeq)
	for idx, it := range items[startSeq:] {
		seq := startSeq + idx + 1
		if it.event {
			st = statemachine.ResumeAsTightened(st)
			entries = append(entries, Entry{ItemSeq: seq, Ordinal: ordinal,
				EventID: it.eventID, IsEvent: true, StateAfter: st})
			continue
		}
		next, res, err := statemachine.StepWithFlags(st, payload[it.batchID], plans, refs, smFlags)
		if err != nil {
			return nil, st, err
		}
		st = next
		ordinal = st.Ordinal
		res.Ordinal = ordinal
		entries = append(entries, Entry{ItemSeq: seq, Ordinal: ordinal,
			BatchID: it.batchID, Result: res, StateAfter: st})
	}
	return entries, st, nil
}

// FoldFrom folds the suffix starting at startSeq (0-based), seeded with start.
func FoldFrom(start statemachine.State, plans statemachine.PlanTriple,
	refs map[statemachine.Severity]statemachine.PlanRef, fl Flags,
	tl Timeline, startSeq int) ([]Entry, statemachine.State, error) {
	return fold(start, plans, refs, fl, tl, startSeq)
}

// FullReplay folds from the initial state; this is the reference
// implementation the incremental path is tested against.
func FullReplay(plans statemachine.PlanTriple,
	refs map[statemachine.Severity]statemachine.PlanRef, fl Flags,
	tl Timeline) ([]Entry, statemachine.State, error) {
	return fold(statemachine.InitialState(), plans, refs, fl, tl, 0)
}
