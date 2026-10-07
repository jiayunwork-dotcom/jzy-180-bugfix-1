package replay_test

import (
	"math/rand"
	"reflect"
	"sort"
	"testing"
	"time"

	"qcinspect/internal/replay"
	"qcinspect/internal/sampling"
	"qcinspect/internal/statemachine"
)

func triple() statemachine.PlanTriple {
	return statemachine.PlanTriple{
		Normal:    sampling.Plan{Kind: sampling.Single, N: 8, C: 3},
		Tightened: sampling.Plan{Kind: sampling.Single, N: 8, C: 1},
		Reduced:   sampling.Plan{Kind: sampling.Single, N: 8, C: 5},
	}
}

func refs() map[statemachine.Severity]statemachine.PlanRef {
	tr := triple()
	return map[statemachine.Severity]statemachine.PlanRef{
		statemachine.Normal:    {PlanID: 1, Plan: tr.Normal},
		statemachine.Tightened: {PlanID: 2, Plan: tr.Tightened},
		statemachine.Reduced:   {PlanID: 3, Plan: tr.Reduced},
	}
}

// foldPrefix manually walks a timeline with the state machine so the test can
// snapshot states at arbitrary cut points.
func TestCheckpointSeedingEquivalence(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	tr := triple()
	rf := refs()
	fl := replay.Flags{ProductionStable: true, SupervisorApproval: true}
	for trial := 0; trial < 200; trial++ {
		tl := randomTimeline(rng, 40, fl)
		full, finalFull, err := replay.FullReplay(tr, rf, tl)
		if err != nil {
			// A timeline that leaves the stream suspended then adds more
			// lots is rejected identically by every folding path; skip it.
			continue
		}
		n := len(full)
		if n == 0 {
			continue
		}
		// Cut at a random point and seed the suffix fold with the exact state
		// after the prefix, as a checkpoint would.
		cut := rng.Intn(n)
		prefixState, prefixFlags := stateAfterN(tr, rf, tl, cut)
		suffix, finalSuffix, err := replay.FoldFrom(prefixState, tr, rf, prefixFlags, tl, cut)
		if err != nil {
			t.Fatalf("trial %d suffix: %v", trial, err)
		}
		if len(suffix) != n-cut {
			t.Fatalf("trial %d: suffix len %d want %d", trial, len(suffix), n-cut)
		}
		// Compare each suffix entry against the full fold.
		for i := range suffix {
			a, b := suffix[i].Result, full[cut+i].Result
			if !reflect.DeepEqual(a, b) {
				t.Fatalf("trial %d mismatch at %d:\nfull=%+v\nsuffix=%+v", trial, cut+i, b, a)
			}
		}
		if !reflect.DeepEqual(finalSuffix, finalFull) {
			t.Fatalf("trial %d: final state differs\nfull=%+v\nsuffix=%+v",
				trial, finalFull, finalSuffix)
		}
	}
}

// stateAfterN folds the first n timeline items with the state machine and
// returns the state (replaying resume/flags events too), plus the flags in
// effect immediately after the cut.
func stateAfterN(tr statemachine.PlanTriple, rf map[statemachine.Severity]statemachine.PlanRef,
	tl replay.Timeline, n int) (statemachine.State, replay.Flags) {
	entries, _, err := replay.FoldFrom(statemachine.InitialState(), tr, rf, tl.InitialFlags, tl, 0)
	if err != nil {
		panic(err)
	}
	if n == 0 {
		return statemachine.InitialState(), tl.InitialFlags
	}
	flags := tl.InitialFlags
	// FullReplay does not expose per-event flag changes; re-derive the flags
	// at the cut from the timeline directly.
	flags = flagsAtCut(tl, n)
	return entries[n-1].StateAfter, flags
}

func flagsAtCut(tl replay.Timeline, n int) replay.Flags {
	// Build the same ordered items the fold uses by folding twice is not
	// possible from outside; instead sort event times manually using the same
	// ordering rule via the public Fold result: the flags after the nth item
	// equal the last FlagsEvent up to that position, batches and resumes
	// ordered together. Simpler: ask FoldFrom for entries and track flags by
	// scanning FlagsEvents with an independent chronological walk below.
	type point struct {
		at    time.Time
		event bool
		id    int64
		fl    *replay.Flags
	}
	var pts []point
	for i := range tl.FlagsEvents {
		e := tl.FlagsEvents[i]
		t, _ := time.Parse(time.RFC3339Nano, e.OccurredAt)
		f := e.Flags
		pts = append(pts, point{at: t, event: true, id: e.ID, fl: &f})
	}
	for _, e := range tl.ResumeEvents {
		t, _ := time.Parse(time.RFC3339Nano, e.OccurredAt)
		pts = append(pts, point{at: t, event: true, id: e.ID})
	}
	for _, b := range tl.Batches {
		t, _ := time.Parse(time.RFC3339Nano, b.InspectedAt)
		pts = append(pts, point{at: t, id: b.ID})
	}
	sort.SliceStable(pts, func(i, j int) bool {
		if !pts[i].at.Equal(pts[j].at) {
			return pts[i].at.Before(pts[j].at)
		}
		if pts[i].event != pts[j].event {
			return !pts[i].event
		}
		return pts[i].id < pts[j].id
	})
	fl := tl.InitialFlags
	for k := 0; k < n && k < len(pts); k++ {
		if pts[k].fl != nil {
			fl = *pts[k].fl
		}
	}
	return fl
}

func randomTimeline(rng *rand.Rand, max int, initial replay.Flags) replay.Timeline {
	base := time.Date(2026, 1, 1, 8, 0, 0, 0, time.UTC)
	n := 1 + rng.Intn(max)
	tl := replay.Timeline{InitialFlags: initial}
	for i := 0; i < n; i++ {
		at := base.Add(time.Duration(rng.Intn(600)) * time.Hour)
		// Mostly batches; occasionally a resume or flags event.
		switch rng.Intn(14) {
		case 0:
			tl.ResumeEvents = append(tl.ResumeEvents, replay.ResumeEvent{
				ID:         int64(10000 + 3*i),
				OccurredAt: at.UTC().Format(time.RFC3339Nano),
			})
			continue
		case 1:
			tl.FlagsEvents = append(tl.FlagsEvents, replay.FlagsEvent{
				ID:         int64(10000 + 3*i + 1),
				OccurredAt: at.UTC().Format(time.RFC3339Nano),
				Flags: replay.Flags{
					ProductionStable:   rng.Intn(2) == 0,
					SupervisorApproval: rng.Intn(2) == 0,
				},
			})
			continue
		}
		// Counts biased to produce accepted and rejected lots on all plans.
		d := rng.Intn(9)
		tl.Batches = append(tl.Batches, statemachine.BatchInput{
			ID:          int64(i + 1),
			BatchNo:     "L" + itoa(i),
			InspectedAt: at.UTC().Format(time.RFC3339Nano),
			D1:          d,
		})
	}
	return tl
}

func itoa(i int) string {
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
