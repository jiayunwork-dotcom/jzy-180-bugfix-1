package replay_test

import (
	"testing"

	"qcinspect/internal/replay"
	"qcinspect/internal/statemachine"
)

// TestFlagEventsOnlyAffectLaterLots checks at the fold level that flag
// changes are timeline events: earlier batches keep the old flags, and only
// batches after a flag event see the new value.
func TestFlagEventsOnlyAffectLaterLots(t *testing.T) {
	tr := triple()
	rf := refs()
	// all-accept single plans (c=3, d=0): +3 each, switch to reduced at 30
	// on the 10th accepted lot WHEN flags are on.
	at := func(h int) string {
		return hts(2026, 3, 1, h, 0, 0)
	}
	tl := replay.Timeline{}
	for i := 1; i <= 12; i++ {
		tl.Batches = append(tl.Batches, statemachine.BatchInput{
			ID: int64(i), BatchNo: "L", InspectedAt: at(i), D1: 0,
		})
	}
	// Initial flags OFF; a flags event at 12:00 turns both on.
	tl.FlagEvents = append(tl.FlagEvents, replay.FlagEvent{
		ID: 1, OccurredAt: at(12), ProductionStable: true, SupervisorApproval: true,
	})

	entries, final, err := replay.FullReplay(tr, rf, replay.Flags{}, tl)
	if err != nil {
		t.Fatal(err)
	}
	batchByID := map[int64]statemachine.BatchResult{}
	for _, e := range entries {
		if !e.IsEvent {
			batchByID[e.BatchID] = e.Result
		}
	}
	// L1..L12 must be entirely normal, no transition, score 3..36.
	for i := 1; i <= 12; i++ {
		r := batchByID[int64(i)]
		if r.Severity != statemachine.Normal || r.Transition != statemachine.TransNone {
			t.Fatalf("L%d sev=%s tr=%s, must stay normal/none after a later flag event",
				i, r.Severity, r.Transition)
		}
		if r.Score != 3*i {
			t.Fatalf("L%d score=%d want %d", i, r.Score, 3*i)
		}
	}
	if final.Severity != statemachine.Normal || final.Score != 36 {
		t.Fatalf("final=%s score=%d want normal/36", final.Severity, final.Score)
	}

	// Append L13 after the event: judged normal, carries to_reduced.
	tl.Batches = append(tl.Batches, statemachine.BatchInput{
		ID: 13, BatchNo: "L", InspectedAt: at(13), D1: 0,
	})
	entries, final, err = replay.FullReplay(tr, rf, replay.Flags{}, tl)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.BatchID == 13 {
			if e.Result.Severity != statemachine.Normal || e.Result.Transition != statemachine.TransToReduced {
				t.Fatalf("L13 sev=%s tr=%s want normal/to_reduced",
					e.Result.Severity, e.Result.Transition)
			}
		}
	}
	if final.Severity != statemachine.Reduced {
		t.Fatalf("after L13 final=%s want reduced", final.Severity)
	}
}

// TestFlagTieOrder: a batch at the same instant as a flag event precedes it;
// several flag events at one boundary apply in ascending id order.
func TestFlagTieOrder(t *testing.T) {
	tr := triple()
	rf := refs()
	at := hts(2026, 3, 1, 12, 0, 0)
	tl := replay.Timeline{
		Batches: []statemachine.BatchInput{{ID: 1, BatchNo: "L", InspectedAt: at, D1: 0}},
		FlagEvents: []replay.FlagEvent{
			{ID: 1, OccurredAt: at, ProductionStable: false, SupervisorApproval: false},
			{ID: 2, OccurredAt: at, ProductionStable: true, SupervisorApproval: true},
		},
	}
	entries, _, err := replay.FullReplay(tr, rf, replay.Flags{}, tl)
	if err != nil {
		t.Fatal(err)
	}
	if entries[0].IsEvent {
		t.Fatal("batch must precede events at the same instant")
	}
	if entries[0].Result.Severity != statemachine.Normal {
		t.Fatal("boundary batch judged with flags already on")
	}
	// Events then flags-off (id1), flags-on (id2): final entry flags on.
	if entries[2].Kind != "flags" || !entries[2].FlagsAfter.ProductionStable {
		t.Fatalf("last entry=%v flags=%v, want flags(on)", entries[2].Kind, entries[2].FlagsAfter)
	}
}

// TestFlagEventCheckpointSeeding: a suffix seeded from a checkpoint at the
// flag-event position reconstructs the flags in force there, so a batch right
// after is judged with the new flags (this guards the flagsAt seed rule).
func TestFlagEventCheckpointSeeding(t *testing.T) {
	tr := triple()
	rf := refs()
	at := func(h int) string { return hts(2026, 3, 1, h, 0, 0) }
	tl := replay.Timeline{
		Batches: []statemachine.BatchInput{
			{ID: 1, InspectedAt: at(10), D1: 0},
			{ID: 2, InspectedAt: at(11), D1: 0},
		},
		FlagEvents: []replay.FlagEvent{
			{ID: 9, OccurredAt: at(10), ProductionStable: true, SupervisorApproval: true},
		},
	}
	// Order at hour 10: batch1, flag event. Full fold: batch1 +3 (flags off),
	// event flips flags, batch11 +3 -> score 6.
	full, finalFull, err := replay.FullReplay(tr, rf, replay.Flags{}, tl)
	if err != nil {
		t.Fatal(err)
	}
	// Seed suffix at position 2 (the flag event itself) with the state after
	// position 2, folding only item 3 (batch at 11). The flags must be ON.
	seedState := full[1].StateAfter
	suffix, finalSuffix, err := replay.FoldFrom(seedState, tr, rf, replay.Flags{}, tl, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(suffix) != 1 || suffix[0].BatchID != 2 {
		t.Fatalf("suffix=%+v", suffix)
	}
	if !suffix[0].FlagsAfter.ProductionStable {
		t.Fatal("suffix fold did not reconstruct the flags-on state from the seed position")
	}
	if finalFull.Score != finalSuffix.Score || finalFull.Severity != finalSuffix.Severity {
		t.Fatalf("full=%+v suffix=%+v", finalFull, finalSuffix)
	}
}

// hts builds an RFC3339 timestamp.
func hts(year, month, day, hour, min, sec int) string {
	return itoa(year) + "-" + pad(month) + "-" + pad(day) + "T" +
		pad(hour) + ":" + pad(min) + ":" + pad(sec) + "Z"
}

func pad(i int) string {
	if i < 10 {
		return "0" + itoa3(i)
	}
	return itoa3(i)
}

func itoa3(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}
