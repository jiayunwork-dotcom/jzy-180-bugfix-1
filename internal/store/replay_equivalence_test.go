package store_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"testing"

	"qcinspect/internal/statemachine"
	"qcinspect/internal/store"
)

// TestRandomHistoryEdits performs a long sequence of backdated inserts,
// deletes, count/time corrections, flag flips and resume events and, after
// every single mutation, compares the store's current state and every
// per-batch record (severity, plan snapshot, decision, score, transition,
// ordinal, second-sample marker) against a fresh full replay of all stored
// rows. This is the required "incremental vs from-scratch replay"
// equivalence proof under the new point-in-time flag semantics.
func TestRandomHistoryEdits(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	for _, seed := range []int64{20261001, 20261002, 20261003, 20261007} {
		seed := seed
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			randomHistoryEdits(ctx, t, st, seed)
		})
	}
}

func randomHistoryEdits(ctx context.Context, t *testing.T, st *store.Store, seed int64) {
	t.Helper()
	suffix := fmt.Sprintf("rand%d", seed)
	nID, tID, rID := mkTriplePlans(t, st, suffix)
	sr, err := st.CreateStream(ctx, store.CreateStreamParams{
		Name: "rand-stream-" + suffix, NormalPlanID: nID, TightenedPlanID: tID, ReducedPlanID: rID,
	})
	if err != nil {
		t.Fatal(err)
	}
	sid := sr.ID

	rng := rand.New(rand.NewSource(seed))

	type rec struct {
		id   int64
		hour int
		d1   int
	}
	var live []rec
	seq := 0
	maxD := 8 // must be <= every plan's sample size

	// Seed with a few in-order accepted lots.
	for i := 0; i < 3; i++ {
		hour := 100 + i
		mr, err := st.AddBatch(ctx, sid, store.BatchInput{
			BatchNo:     lotName(int(seed) + seq),
			InspectedAt: at(hour),
			D1:          0,
		})
		seq++
		if err != nil {
			t.Fatal(err)
		}
		live = append(live, rec{id: newestID(mr), hour: hour, d1: 0})
	}
	verifyAgainstReference(ctx, t, st, sid)

	steps := 400
	for step := 0; step < steps; step++ {
		kind := rng.Intn(100)
		switch {
		case kind < 55: // insert (often backdated)
			hour := 50 + rng.Intn(500)
			d := rng.Intn(maxD + 1)
			mr, err := st.AddBatch(ctx, sid, store.BatchInput{
				BatchNo:     lotName(int(seed) + seq),
				InspectedAt: at(hour),
				D1:          d,
			})
			seq++
			if errors.Is(err, store.ErrSuspended) {
				// Rolled back (the lot falls in a suspended period).
			} else if err != nil {
				t.Fatalf("step %d insert: %v", step, err)
			} else {
				live = append(live, rec{id: newestID(mr), hour: hour, d1: d})
			}
		case kind < 75 && len(live) > 1: // correct a batch count (same time)
			i := rng.Intn(len(live))
			newD := rng.Intn(maxD + 1)
			b := live[i]
			_, err := st.UpdateBatch(ctx, sid, b.id, store.BatchInput{
				BatchNo:     lotName(int(seed) + 100000 + int(b.id)),
				InspectedAt: at(b.hour),
				D1:          newD,
			})
			if errors.Is(err, store.ErrSuspended) {
				// Rolled back.
			} else if err != nil {
				t.Fatalf("step %d update: %v", step, err)
			} else {
				live[i].d1 = newD
			}
		case kind < 83 && len(live) > 1: // move a batch in time (back/forward)
			i := rng.Intn(len(live))
			b := live[i]
			newHour := 50 + rng.Intn(500)
			_, err := st.UpdateBatch(ctx, sid, b.id, store.BatchInput{
				BatchNo:     lotName(int(seed) + 200000 + int(b.id)),
				InspectedAt: at(newHour),
				D1:          b.d1,
			})
			if errors.Is(err, store.ErrSuspended) {
				// Rolled back.
			} else if err != nil {
				t.Fatalf("step %d move: %v", step, err)
			} else {
				live[i].hour = newHour
			}
		case kind < 91 && len(live) >= 1: // delete (possibly the last batch)
			i := rng.Intn(len(live))
			b := live[i]
			if _, err := st.DeleteBatch(ctx, sid, b.id); errors.Is(err, store.ErrSuspended) {
				// Rolled back.
			} else if err != nil {
				t.Fatalf("step %d delete: %v", step, err)
			} else {
				live = append(live[:i], live[i+1:]...)
			}
		case kind < 95: // toggle flags (takes effect strictly after the current latest batch)
			stable := rng.Intn(2) == 0
			approval := rng.Intn(2) == 0
			// Toggle the flags independently often enough to exercise partial
			// PATCHes and the no-batches/from-start branch.
			var ps, pa *bool
			if rng.Intn(2) == 0 {
				ps = &stable
			}
			if rng.Intn(2) == 0 {
				pa = &approval
			}
			if ps == nil && pa == nil {
				ps = &stable
			}
			if _, err := st.SetFlags(ctx, sid, ps, pa); err != nil {
				t.Fatalf("step %d flags: %v", step, err)
			}
		default: // arbitrary resume event interleaved into the timeline
			hour := 50 + rng.Intn(600)
			if _, err := st.Resume(ctx, sid, at(hour)); err != nil && !errors.Is(err, store.ErrSuspended) {
				t.Fatalf("step %d resume: %v", step, err)
			}
		}
		// A rejected edit (a lot folding into a suspended period) is rolled
		// back and changes nothing. If the *current* state is suspended,
		// record a resume event after the newest batch so subsequent inserts
		// are legal again.
		detail, derr := st.GetStreamDetail(ctx, sid)
		if derr != nil {
			t.Fatal(derr)
		}
		curState, ok := detail.State.(statemachine.State)
		if !ok {
			t.Fatalf("unexpected state type %T", detail.State)
		}
		if curState.Severity == statemachine.Suspended {
			newest := 0
			for _, b := range live {
				if b.hour > newest {
					newest = b.hour
				}
			}
			if _, err := st.Resume(ctx, sid, at(newest+100)); err != nil {
				t.Fatalf("step %d resume: %v", step, err)
			}
		}

		verifyAgainstReference(ctx, t, st, sid)
	}
}

func newestID(mr store.MutationResult) int64 {
	var max int64
	for _, b := range mr.All {
		if b.ID > max {
			max = b.ID
		}
	}
	return max
}

func lotName(i int) string {
	return "LOT-" + itoa2(i)
}

func itoa2(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	if neg {
		b = append([]byte{'-'}, b...)
	}
	return string(b)
}
