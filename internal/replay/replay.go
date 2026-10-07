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
//
// Timeline semantics. The control flags are NOT a constant over the replay:
// every PATCH that changes them while the stream already has batches appends a
// "flags" event positioned immediately AFTER the batch with the then-latest
// inspected_at. Batches up to and including that boundary keep the old flags;
// everything after the event is judged with the new flags. Backdating a batch
// therefore naturally re-judges it under the flags that were in effect at its
// point in time. Flags given at stream creation (and any PATCH made while the
// stream still has no batches) are the InitialFlags and apply from the very
// beginning. Folding the stream in timeline order is thus the complete
// definition of correctness, regardless of the wall-clock order in which
// rows were written.
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

// FlagsEvent is a point-in-time change of the stream control flags. Only
// batches positioned strictly after it are judged under the new flags.
type FlagsEvent struct {
	ID         int64
	OccurredAt string // RFC3339 timestamp; equals the boundary batch's time
	Flags      Flags
}

// Flags are stream-wide control inputs at one point in the timeline.
type Flags struct {
	ProductionStable   bool
	SupervisorApproval bool
}

// item is the unified timeline element.
type item struct {
	at      string
	batchID int64
	eventID int64 // id in stream_events, shared by resume and flags events
	kind    itemKind
}

type itemKind int

const (
	kindBatch itemKind = iota
	kindResume
	kindFlags
)

// Entry is one folded timeline element in output order.
type Entry struct {
	ItemSeq int // 1-based position across batches and events
	Ordinal int // 1-based batch ordinal (events do not advance it)
	BatchID int64
	EventID int64 // resume or flags event id, depending on IsEvent/FlagsEvent
	IsEvent bool
	// FlagsEvent marks an IsEvent entry that carries a flag change (the
	// alternative is a resume event).
	FlagsEvent bool
	Result     statemachine.BatchResult
	// StateAfter is the complete state immediately after this element; it is
	// what a checkpoint at ItemSeq stores.
	StateAfter statemachine.State
}

// Timeline orders batches and administrative events for folding.
type Timeline struct {
	Batches []statemachine.BatchInput
	// ResumeEvents force tightened inspection at their point in time.
	ResumeEvents []ResumeEvent
	// FlagsEvents change the control flags for everything after them.
	FlagsEvents []FlagsEvent
	// InitialFlags are the flags in effect before the first FlagsEvent; they
	// come from stream creation (or a PATCH made while no batches existed).
	InitialFlags Flags
}

// items builds the unified ordered list of batches and events.
func (tl Timeline) items() []item {
	out := make([]item, 0, len(tl.Batches)+len(tl.ResumeEvents)+len(tl.FlagsEvents))
	for _, b := range tl.Batches {
		out = append(out, item{at: b.InspectedAt, batchID: b.ID, kind: kindBatch})
	}
	for _, e := range tl.ResumeEvents {
		out = append(out, item{at: e.OccurredAt, eventID: e.ID, kind: kindResume})
	}
	for _, e := range tl.FlagsEvents {
		out = append(out, item{at: e.OccurredAt, eventID: e.ID, kind: kindFlags})
	}
	// Chronological order; ties broken deterministically so backdated inserts
	// never make the fold ambiguous:
	//   1. batches precede every kind of event at the same instant (a flags
	//      event written at a boundary batch's time must cover exactly the
	//      batches strictly after that batch, so every batch sharing the
	//      boundary timestamp is still folded under the old flags);
	//   2. administrative events follow their stream_events id (BIGSERIAL:
	//      the wall-clock order in which the PATCHes/resumes were committed),
	//      resume and flags events sharing one id space.
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].at != out[j].at {
			return out[i].at < out[j].at
		}
		if out[i].kind != out[j].kind {
			if out[i].kind == kindBatch || out[j].kind == kindBatch {
				return out[i].kind == kindBatch
			}
			return out[i].eventID < out[j].eventID
		}
		if out[i].kind == kindBatch {
			return out[i].batchID < out[j].batchID
		}
		return out[i].eventID < out[j].eventID
	})
	return out
}

// fold folds timeline items starting from item index startSeq (0-based; items
// before it are skipped and must match the checkpoint that produced start).
// Returned entries cover the suffix and the final machine state.
//
// The flags in effect at the fold's start must be passed in startFlags:
// checkpoints store machine state only, and flag state is re-derived by the
// caller from the FlagsEvents before startSeq.
func fold(start statemachine.State, plans statemachine.PlanTriple,
	refs map[statemachine.Severity]statemachine.PlanRef, startFlags Flags,
	tl Timeline, startSeq int) ([]Entry, statemachine.State, error) {

	items := tl.items()
	payload := make(map[int64]statemachine.BatchInput, len(tl.Batches))
	for _, b := range tl.Batches {
		payload[b.ID] = b
	}
	flagValues := make(map[int64]Flags, len(tl.FlagsEvents))
	for _, e := range tl.FlagsEvents {
		flagValues[e.ID] = e.Flags
	}
	fl := startFlags
	st := start
	ordinal := st.Ordinal
	if startSeq < 0 {
		startSeq = 0
	}
	entries := make([]Entry, 0, len(items)-startSeq)
	for idx, it := range items[startSeq:] {
		seq := startSeq + idx + 1
		switch it.kind {
		case kindResume:
			st = statemachine.ResumeAsTightened(st)
			entries = append(entries, Entry{ItemSeq: seq, Ordinal: ordinal,
				EventID: it.eventID, IsEvent: true, StateAfter: st})
		case kindFlags:
			fl = flagValues[it.eventID]
			entries = append(entries, Entry{ItemSeq: seq, Ordinal: ordinal,
				EventID: it.eventID, IsEvent: true, FlagsEvent: true, StateAfter: st})
		default:
			smFlags := statemachine.FlagsExported{
				ProductionStable:   fl.ProductionStable,
				SupervisorApproval: fl.SupervisorApproval,
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
	}
	return entries, st, nil
}

// FoldFrom folds the suffix starting at startSeq (0-based), seeded with start
// and with the flags already in effect at that point. The caller is
// responsible for replaying the earlier FlagsEvents to obtain startFlags.
func FoldFrom(start statemachine.State, plans statemachine.PlanTriple,
	refs map[statemachine.Severity]statemachine.PlanRef, startFlags Flags,
	tl Timeline, startSeq int) ([]Entry, statemachine.State, error) {
	return fold(start, plans, refs, startFlags, tl, startSeq)
}

// FullReplay folds from the initial state with the stream's initial flags;
// this is the reference implementation the incremental path is tested
// against.
func FullReplay(plans statemachine.PlanTriple,
	refs map[statemachine.Severity]statemachine.PlanRef, tl Timeline) ([]Entry, statemachine.State, error) {
	return fold(statemachine.InitialState(), plans, refs, tl.InitialFlags, tl, 0)
}
