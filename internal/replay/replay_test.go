package replay_test

import (
	"math/rand"
	"reflect"
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
		tl := randomTimeline(rng, 40)
		full, finalFull, err := replay.FullReplay(tr, rf, fl, tl)
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
		prefixState := stateAfterN(tr, rf, fl, tl, cut)
		suffix, finalSuffix, err := replay.FoldFrom(prefixState, tr, rf, fl, tl, cut)
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
// returns the state (replaying resume events too).
func stateAfterN(tr statemachine.PlanTriple, rf map[statemachine.Severity]statemachine.PlanRef,
	fl replay.Flags, tl replay.Timeline, n int) statemachine.State {
	// Build an ordered list by folding whole timeline with a limiter using
	// FoldFrom on slices: easiest is to reproduce ordering from the public
	// result states returned per entry (StateAfter).
	entries, _, err := replay.FoldFrom(statemachine.InitialState(), tr, rf, fl, tl, 0)
	if err != nil {
		panic(err)
	}
	if n == 0 {
		return statemachine.InitialState()
	}
	return entries[n-1].StateAfter
}

func randomTimeline(rng *rand.Rand, max int) replay.Timeline {
	base := time.Date(2026, 1, 1, 8, 0, 0, 0, time.UTC)
	n := 1 + rng.Intn(max)
	tl := replay.Timeline{}
	for i := 0; i < n; i++ {
		at := base.Add(time.Duration(rng.Intn(600)) * time.Hour)
		// Mostly batches; occasionally a resume event.
		if rng.Intn(12) == 0 {
			tl.Events = append(tl.Events, replay.ResumeEvent{
				ID:         int64(10000 + i),
				OccurredAt: at.UTC().Format(time.RFC3339Nano),
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
