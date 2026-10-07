// Package replay folds the complete history of a stream (batches plus
// administrative events: resumes and point-in-time control-flag changes) into
// per-batch records and the current state.
//
// Strategy: incremental recompute from a checkpoint, NOT full replay on every
// edit and NOT purely incremental state patching. The fold itself lives in
// statemachine.StepWithFlags and is a pure function, so recomputing a suffix
// from a persisted checkpoint state is exactly the same computation as folding
// the whole history from InitialState; checkpoints are only an optimization.
//
// Flag handling: the initial flags (in force at the timeline start) are the
// FoldFrom argument; FlagEvents carry complete post-change flag states and are
// themselves timeline elements. The flags active at a checkpoint-seeded suffix
// start are reconstructed by replaying flag events up to and including the
// seed position, so a checkpoint stores the machine state only.
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

// FlagEvent is a point-in-time change of the stream control flags. It carries
// the COMPLETE post-change flag state and applies only to batches later in
// timeline order: batches at or before its position keep being judged with the
// flags previously in force. Flag changes are therefore part of the timeline
// just like batches and resume events.
type FlagEvent struct {
	ID                 int64
	OccurredAt         string // RFC3339 timestamp
	ProductionStable   bool
	SupervisorApproval bool
}

// Flags are the stream control inputs at the timeline start. In a fold they
// are the INITIAL flag state; FlagEvents update them as the fold proceeds.
type Flags struct {
	ProductionStable   bool
	SupervisorApproval bool
}

// item is the unified timeline element.
type item struct {
	at        string
	batchID   int64
	eventID   int64
	resume    bool
	flagEvent bool
	flag      FlagEvent
}

// Entry is one folded timeline element in output order.
type Entry struct {
	ItemSeq int // 1-based position across batches and events
	Ordinal int // 1-based batch ordinal (events do not advance it)
	BatchID int64
	EventID int64
	IsEvent bool
	// Kind is "resume" or "flags" for event entries; empty for batches.
	Kind   string
	Result statemachine.BatchResult
	// FlagsAfter records the control flags in force after this element. For
	// flag events it is the newly set value; events leave the folded state
	// untouched otherwise.
	FlagsAfter Flags
	// StateAfter is the complete state immediately after this element; it is
	// what a checkpoint at ItemSeq stores.
	StateAfter statemachine.State
}

// Timeline orders batches, resume events and flag-change events for folding.
type Timeline struct {
	Batches    []statemachine.BatchInput
	Events     []ResumeEvent
	FlagEvents []FlagEvent
}

func (tl Timeline) items() []item {
	out := make([]item, 0, len(tl.Batches)+len(tl.Events)+len(tl.FlagEvents))
	for _, b := range tl.Batches {
		out = append(out, item{at: b.InspectedAt, batchID: b.ID})
	}
	for _, e := range tl.Events {
		out = append(out, item{at: e.OccurredAt, eventID: e.ID, resume: true})
	}
	for _, f := range tl.FlagEvents {
		out = append(out, item{at: f.OccurredAt, eventID: f.ID, flagEvent: true, flag: f})
	}
	// Chronological order; ties broken with a total order:
	//   1. batches before any event at the same instant (a batch backfilled
	//      exactly at a flag boundary is still judged with the flags in force
	//      up to that instant — the change takes effect only after it);
	//   2. resume events before flag events;
	//   3. ascending source id. Consecutive flag changes submitted without
	//      newer batches share one boundary and then order newest-last, so
	//      the most recent submission is the one in force after that
	//      boundary.
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].at != out[j].at {
			return out[i].at < out[j].at
		}
		ci, cj := out[i].eventClass(), out[j].eventClass()
		if ci != cj {
			return ci < cj
		}
		if out[i].batchID != out[j].batchID {
			return out[i].batchID < out[j].batchID
		}
		return out[i].eventID < out[j].eventID
	})
	return out
}

// eventClass orders batches (0) before resume events (1) before flag events
// (2) at equal timestamps.
func (it item) eventClass() int {
	switch {
	case it.flagEvent:
		return 2
	case it.resume:
		return 1
	default:
		return 0
	}
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
	// The active flags are not part of a checkpoint. They are reconstructed
	// from the initial flags and every flag event at or before the seed
	// position: a checkpoint at position N is the state immediately AFTER
	// folding item N, so a flag event sitting exactly at N is already in
	// force.
	active := flagsAt(items, fl, startSeq+1)
	st := start
	ordinal := st.Ordinal
	if startSeq < 0 {
		startSeq = 0
	}
	entries := make([]Entry, 0, len(items)-startSeq)
	for idx, it := range items[startSeq:] {
		seq := startSeq + idx + 1
		switch {
		case it.flagEvent:
			active = Flags{
				ProductionStable:   it.flag.ProductionStable,
				SupervisorApproval: it.flag.SupervisorApproval,
			}
			entries = append(entries, Entry{ItemSeq: seq, Ordinal: ordinal,
				EventID: it.eventID, IsEvent: true, Kind: "flags",
				FlagsAfter: active, StateAfter: st})
		case it.resume:
			st = statemachine.ResumeAsTightened(st)
			entries = append(entries, Entry{ItemSeq: seq, Ordinal: ordinal,
				EventID: it.eventID, IsEvent: true, Kind: "resume",
				FlagsAfter: active, StateAfter: st})
		default:
			next, res, err := statemachine.StepWithFlags(st, payload[it.batchID], plans, refs,
				statemachine.FlagsExported{
					ProductionStable:   active.ProductionStable,
					SupervisorApproval: active.SupervisorApproval,
				})
			if err != nil {
				return nil, st, err
			}
			st = next
			ordinal = st.Ordinal
			res.Ordinal = ordinal
			entries = append(entries, Entry{ItemSeq: seq, Ordinal: ordinal,
				BatchID: it.batchID, Result: res, FlagsAfter: active, StateAfter: st})
		}
	}
	return entries, st, nil
}

// flagsAt returns the flags in force immediately before folding the item at
// index startSeq (0-based): the initial flags with every flag event before
// startSeq applied in timeline order. The fold caller passes startSeq+1 so
// that a flag event at the seed position itself is applied (a checkpoint at
// position N is the state AFTER item N).
func flagsAt(items []item, initial Flags, startSeq int) Flags {
	active := initial
	limit := startSeq
	if limit > len(items) {
		limit = len(items)
	}
	if limit < 0 {
		limit = 0
	}
	for i := 0; i < limit; i++ {
		if items[i].flagEvent {
			active = Flags{
				ProductionStable:   items[i].flag.ProductionStable,
				SupervisorApproval: items[i].flag.SupervisorApproval,
			}
		}
	}
	return active
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
