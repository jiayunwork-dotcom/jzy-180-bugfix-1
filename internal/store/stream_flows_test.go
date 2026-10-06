package store_test

import (
	"context"
	"testing"

	"qcinspect/internal/statemachine"
	"qcinspect/internal/store"
)

// TestStreamTransitionSequences builds the exact sequences that must trigger
// each transition and checks the persisted per-batch markers.
func TestStreamTransitionSequences(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	nID, tID, rID := mkTriplePlans(t, st, "seq")

	t.Run("normal to tightened", func(t *testing.T) {
		sr, _ := st.CreateStream(ctx, store.CreateStreamParams{
			Name: "s-t", NormalPlanID: nID, TightenedPlanID: tID, ReducedPlanID: rID})
		mr, err := st.AddBatch(ctx, sr.ID, store.BatchInput{BatchNo: "a", InspectedAt: at(1), D1: 4})
		if err != nil {
			t.Fatal(err)
		}
		mr, err = st.AddBatch(ctx, sr.ID, store.BatchInput{BatchNo: "b", InspectedAt: at(2), D1: 0})
		if err != nil {
			t.Fatal(err)
		}
		mr, err = st.AddBatch(ctx, sr.ID, store.BatchInput{BatchNo: "c", InspectedAt: at(3), D1: 4})
		if err != nil {
			t.Fatal(err)
		}
		last := mr.All[len(mr.All)-1]
		if last.Result["transition"] != "to_tightened" {
			t.Fatalf("transition=%v", last.Result["transition"])
		}
		if mr.State.Severity != statemachine.Tightened {
			t.Fatalf("severity=%s", mr.State.Severity)
		}
	})

	t.Run("tightened to normal then reduced", func(t *testing.T) {
		sr, _ := st.CreateStream(ctx, store.CreateStreamParams{
			Name: "s-tr", NormalPlanID: nID, TightenedPlanID: tID, ReducedPlanID: rID,
			ProductionStable: true, SupervisorApproval: true})
		// Reach tightened (normal c=3, d=4 rejects).
		for i, d := range []int{4, 4} {
			if _, err := st.AddBatch(ctx, sr.ID, store.BatchInput{
				BatchNo: string(rune('a' + i)), InspectedAt: at(i + 1), D1: d}); err != nil {
				t.Fatal(err)
			}
		}
		// Five accepted on tightened (c=1, d=0) -> normal.
		for i := 0; i < 5; i++ {
			mr, err := st.AddBatch(ctx, sr.ID, store.BatchInput{
				BatchNo: "t" + string(rune('a'+i)), InspectedAt: at(10 + i), D1: 0})
			if err != nil {
				t.Fatal(err)
			}
			if i == 4 && mr.All[len(mr.All)-1].Result["transition"] != "to_normal" {
				t.Fatal("5th tightened accept must mark to_normal")
			}
		}
		// 10 perfect normal lots -> score 30 -> reduced (flags both on).
		for i := 0; i < 10; i++ {
			mr, err := st.AddBatch(ctx, sr.ID, store.BatchInput{
				BatchNo: "n" + string(rune('a'+i)), InspectedAt: at(20 + i), D1: 0})
			if err != nil {
				t.Fatal(err)
			}
			if i == 9 {
				if mr.State.Severity != statemachine.Reduced {
					t.Fatalf("severity=%s want reduced", mr.State.Severity)
				}
			}
		}
	})

	t.Run("suspend and manual resume", func(t *testing.T) {
		sr, _ := st.CreateStream(ctx, store.CreateStreamParams{
			Name: "s-sus", NormalPlanID: nID, TightenedPlanID: tID, ReducedPlanID: rID})
		// Reach tightened immediately: reject, reject (normal c=3, d=4).
		if _, err := st.AddBatch(ctx, sr.ID, store.BatchInput{BatchNo: "1", InspectedAt: at(1), D1: 4}); err != nil {
			t.Fatal(err)
		}
		if _, err := st.AddBatch(ctx, sr.ID, store.BatchInput{BatchNo: "2", InspectedAt: at(2), D1: 4}); err != nil {
			t.Fatal(err)
		}
		// On tightened (c=1): alternate R, A to avoid escaping back to normal,
		// accumulating 5 rejects -> suspended.
		resumeHour := 0
		for i := 0; i < 9; i++ {
			d := 0
			if i%2 == 0 {
				d = 2 // reject on tightened
			}
			mr, err := st.AddBatch(ctx, sr.ID, store.BatchInput{
				BatchNo: string(rune('a' + i)), InspectedAt: at(10 + i), D1: d})
			if err != nil {
				t.Fatalf("i=%d: %v", i, err)
			}
			if mr.State.Severity == statemachine.Suspended {
				resumeHour = 10 + i
				break
			}
		}
		detail, _ := st.GetStreamDetail(ctx, sr.ID)
		if detail.State.(statemachine.State).Severity != statemachine.Suspended {
			t.Fatal("expected suspended")
		}
		// Adding a lot now fails.
		if _, err := st.AddBatch(ctx, sr.ID, store.BatchInput{
			BatchNo: "late", InspectedAt: at(500), D1: 0}); err != store.ErrSuspended {
			t.Fatalf("want ErrSuspended, got %v", err)
		}
		// Manual resume restarts tightened; five accepts return to normal.
		if _, err := st.Resume(ctx, sr.ID, at(resumeHour+200)); err != nil {
			t.Fatal(err)
		}
		var last store.MutationResult
		for i := 0; i < 5; i++ {
			last, _ = st.AddBatch(ctx, sr.ID, store.BatchInput{
				BatchNo: string(rune('A' + i)), InspectedAt: at(resumeHour + 300 + i), D1: 0})
		}
		if last.State.Severity != statemachine.Normal {
			t.Fatalf("post-resume severity=%s", last.State.Severity)
		}
	})
}

// TestDoubleSamplingStream drives a double-plan stream, verifying first- and
// second-sample judgments and that d2 is mandatory.
func TestDoubleSamplingStream(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	nID, tID, rID := mkDoublePlans(t, st, "dbl")
	sr, err := st.CreateStream(ctx, store.CreateStreamParams{
		Name: "dbl-s", NormalPlanID: nID, TightenedPlanID: tID, ReducedPlanID: rID})
	if err != nil {
		t.Fatal(err)
	}
	d2zero, d2two, d2three := 0, 2, 3
	add := func(no string, hour, d1 int, d2 *int) store.MutationResult {
		t.Helper()
		mr, err := st.AddBatch(ctx, sr.ID, store.BatchInput{
			BatchNo: no, InspectedAt: at(hour), D1: d1, D2: d2})
		if err != nil {
			t.Fatalf("%s: %v", no, err)
		}
		return mr
	}
	// d2 required.
	if _, err := st.AddBatch(ctx, sr.ID, store.BatchInput{
		BatchNo: "missing", InspectedAt: at(1), D1: 0}); err == nil {
		t.Fatal("double plan must require explicit d2")
	}
	// Normal double plan: c1=1, r1=4, c2=4.
	mr := add("acc1", 2, 1, &d2zero) // accept on first
	if !mr.All[len(mr.All)-1].Result["accepted"].(bool) ||
		mr.All[len(mr.All)-1].Result["second_sample_taken"].(bool) {
		t.Fatal("d1<=c1 should accept without second sample")
	}
	mr = add("rej1", 3, 4, &d2zero) // reject on first
	if mr.All[len(mr.All)-1].Result["accepted"].(bool) ||
		mr.All[len(mr.All)-1].Result["second_sample_taken"].(bool) {
		t.Fatal("d1>=r1 should reject without second sample")
	}
	mr = add("cont-acc", 4, 2, &d2two) // 2+2=4 <= c2 accept after second
	b := mr.All[len(mr.All)-1]
	if !b.Result["accepted"].(bool) || !b.Result["second_sample_taken"].(bool) {
		t.Fatal("continue region then total<=c2 should accept on second sample")
	}
	mr = add("cont-rej", 5, 3, &d2three) // 3+3=6 > c2 reject
	b = mr.All[len(mr.All)-1]
	if b.Result["accepted"].(bool) || !b.Result["second_sample_taken"].(bool) {
		t.Fatal("continue region then total>c2 should reject on second sample")
	}
	// d1 above sample size rejected with field error.
	if _, err := st.AddBatch(ctx, sr.ID, store.BatchInput{
		BatchNo: "bad", InspectedAt: at(6), D1: 9, D2: &d2zero}); err == nil {
		t.Fatal("d1 > n1 must be rejected")
	}
}
