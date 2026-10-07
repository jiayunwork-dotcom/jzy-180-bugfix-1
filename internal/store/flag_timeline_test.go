package store_test

import (
	"context"
	"testing"
	"time"

	"qcinspect/internal/sampling"
	"qcinspect/internal/statemachine"
	"qcinspect/internal/store"
)

// atDay is an inspected_at on 2026-03-01 at the given hour (and minutes).
func atDay(hour, minute int) time.Time {
	return time.Date(2026, 3, 1, hour, minute, 0, 0, time.UTC)
}

// mkBoundaryPlans creates the exact triple from the report:
// normal n=10 c=0, tightened n=10 c=0, reduced n=5 c=0.
func mkBoundaryPlans(t *testing.T, st *store.Store) (int64, int64, int64) {
	t.Helper()
	n := mustPlan(t, st, "norm10", sampling.Plan{Kind: sampling.Single, N: 10, C: 0})
	ti := mustPlan(t, st, "tight10", sampling.Plan{Kind: sampling.Single, N: 10, C: 0})
	r := mustPlan(t, st, "red5", sampling.Plan{Kind: sampling.Single, N: 5, C: 0})
	return n, ti, r
}

func lotByNo(t *testing.T, rows []store.BatchView, no string) store.BatchView {
	t.Helper()
	for _, b := range rows {
		if b.BatchNo == no {
			return b
		}
	}
	t.Fatalf("batch %s not found", no)
	return store.BatchView{}
}

func planIDOf(b store.BatchView) int64 {
	plan, _ := b.Result["plan"].(map[string]interface{})
	id, _ := plan["plan_id"].(float64)
	return int64(id)
}

// TestFlagBoundaryLeavesHistoryUntouched reproduces the exact incident:
// 12 hourly accepted lots L1..L12 with both flags off, then PATCH both flags
// on. Nothing recorded before the boundary may change; the switch to reduced
// can only happen on a batch inspected AFTER the boundary.
func TestFlagBoundaryLeavesHistoryUntouched(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	nID, tID, rID := mkBoundaryPlans(t, st)
	sr, err := st.CreateStream(ctx, store.CreateStreamParams{
		Name: "boundary", NormalPlanID: nID, TightenedPlanID: tID, ReducedPlanID: rID,
	})
	if err != nil {
		t.Fatal(err)
	}

	for i := 1; i <= 12; i++ {
		if _, err := st.AddBatch(ctx, sr.ID, store.BatchInput{
			BatchNo: "L" + itoa2(i), InspectedAt: atDay(i, 0), D1: 0,
		}); err != nil {
			t.Fatalf("L%d: %v", i, err)
		}
	}
	detail, err := st.GetStreamDetail(ctx, sr.ID)
	if err != nil {
		t.Fatal(err)
	}
	before := detail.Batches
	if state := detail.State.(statemachine.State); state.Severity != statemachine.Normal || state.Score != 36 {
		t.Fatalf("before flags: severity=%s score=%d, want normal/36", state.Severity, state.Score)
	}
	snapshot := map[string]map[string]interface{}{}
	for _, b := range before {
		if b.Result["severity"] != "normal" {
			t.Fatalf("L%s judged %s before flags", b.BatchNo, b.Result["severity"])
		}
		snapshot[b.BatchNo] = b.Result
	}

	// PATCH both flags on; boundary is L12 @12:00.
	on := true
	mr, err := st.SetFlags(ctx, sr.ID, &on, &on)
	if err != nil {
		t.Fatal(err)
	}
	if mr.State.Severity != statemachine.Normal || mr.State.Score != 36 {
		t.Fatalf("after flags: severity=%s score=%d, want normal/36", mr.State.Severity, mr.State.Score)
	}
	if len(mr.All) != 12 {
		t.Fatalf("12 batches expected, got %d", len(mr.All))
	}
	for _, b := range mr.All {
		prev, ok := snapshot[b.BatchNo]
		if !ok {
			t.Fatalf("unexpected batch %s", b.BatchNo)
		}
		if b.Result["severity"] != "normal" {
			t.Errorf("%s severity changed to %v", b.BatchNo, b.Result["severity"])
		}
		if b.Result["switching_score"] != prev["switching_score"] {
			t.Errorf("%s score changed: %v -> %v", b.BatchNo, prev["switching_score"], b.Result["switching_score"])
		}
		if b.Result["transition"] != "none" {
			t.Errorf("%s transition became %v", b.BatchNo, b.Result["transition"])
		}
		if b.Result["accepted"] != true {
			t.Errorf("%s accepted changed", b.BatchNo)
		}
		if planIDOf(b) != nID {
			t.Errorf("%s plan id changed to %d", b.BatchNo, planIDOf(b))
		}
	}

	// L13 at 13:00 is still judged under normal inspection; after it the
	// score reaches the threshold WITH both flags set, so it transitions to
	// reduced.
	mr, err = st.AddBatch(ctx, sr.ID, store.BatchInput{
		BatchNo: "L13", InspectedAt: atDay(13, 0), D1: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	l13 := lotByNo(t, mr.All, "L13")
	if l13.Result["severity"] != "normal" {
		t.Fatalf("L13 judged under %s, want normal", l13.Result["severity"])
	}
	if l13.Result["transition"] != "to_reduced" {
		t.Fatalf("L13 transition=%v, want to_reduced", l13.Result["transition"])
	}
	if planIDOf(l13) != nID {
		t.Fatalf("L13 must use the normal plan")
	}
	if mr.State.Severity != statemachine.Reduced {
		t.Fatalf("state after L13 = %s, want reduced", mr.State.Severity)
	}

	// L14 is the first lot judged under the reduced plan (n=5).
	mr, err = st.AddBatch(ctx, sr.ID, store.BatchInput{
		BatchNo: "L14", InspectedAt: atDay(14, 0), D1: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	l14 := lotByNo(t, mr.All, "L14")
	if l14.Result["severity"] != "reduced" || planIDOf(l14) != rID {
		t.Fatalf("L14 severity=%v plan=%d, want reduced plan %d",
			l14.Result["severity"], planIDOf(l14), rID)
	}
	if mr.State.Severity != statemachine.Reduced {
		t.Fatalf("state after L14 = %s", mr.State.Severity)
	}
	verifyAgainstReference(ctx, t, st, sr.ID)
}

// TestFlagRevokeOnlyAffectsLaterBatches: after the stream has switched to
// reduced, revoking "production stable" must not rewrite reduced lots already
// judged; the first lot entered after the revocation is judged under normal
// inspection.
func TestFlagRevokeOnlyAffectsLaterBatches(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	nID, tID, rID := mkBoundaryPlans(t, st)
	sr, _ := st.CreateStream(ctx, store.CreateStreamParams{
		Name: "revoke", NormalPlanID: nID, TightenedPlanID: tID, ReducedPlanID: rID,
	})
	// Turn flags on before any batch: applies from the start.
	on := true
	if _, err := st.SetFlags(ctx, sr.ID, &on, &on); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 12; i++ { // L10 switches to reduced
		mr, err := st.AddBatch(ctx, sr.ID, store.BatchInput{
			BatchNo: "R" + itoa2(i), InspectedAt: atDay(i, 0), D1: 0,
		})
		if err != nil {
			t.Fatal(err)
		}
		if i == 12 {
			if mr.State.Severity != statemachine.Reduced {
				t.Fatalf("after R12 severity=%s", mr.State.Severity)
			}
		}
	}
	mr, err := st.AddBatch(ctx, sr.ID, store.BatchInput{
		BatchNo: "R13", InspectedAt: atDay(13, 0), D1: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	r13 := lotByNo(t, mr.All, "R13")
	if r13.Result["severity"] != "reduced" || planIDOf(r13) != rID {
		t.Fatalf("R13 should be judged reduced")
	}

	// Revoke production stable. Boundary is R13 @13:00; R1..R13 stay as they
	// were (no retroactive rewriting).
	off := false
	mr, err = st.SetFlags(ctx, sr.ID, &off, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range mr.All {
		wantSev := "normal"
		hour := b.InspectedAt.Hour()
		if hour >= 10 {
			wantSev = "reduced" // R10 triggered the switch; R10 itself normal
		}
		if hour == 10 && b.BatchNo == "R10" {
			wantSev = "normal"
		}
		if b.Result["severity"] != wantSev {
			t.Errorf("%s severity=%v want %s", b.BatchNo, b.Result["severity"], wantSev)
		}
	}
	if mr.State.Severity != statemachine.Reduced {
		t.Fatalf("current severity must remain reduced until a later batch: %s", mr.State.Severity)
	}

	// First batch after the revocation is judged under normal inspection,
	// with a visible to_normal transition.
	mr, err = st.AddBatch(ctx, sr.ID, store.BatchInput{
		BatchNo: "R14", InspectedAt: atDay(14, 0), D1: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	r14 := lotByNo(t, mr.All, "R14")
	if r14.Result["severity"] != "normal" || planIDOf(r14) != nID {
		t.Fatalf("R14 severity=%v plan=%d, want normal/%d",
			r14.Result["severity"], planIDOf(r14), nID)
	}
	if r14.Result["transition"] != "to_normal" {
		t.Fatalf("R14 transition=%v want to_normal", r14.Result["transition"])
	}
	if mr.State.Severity != statemachine.Normal {
		t.Fatalf("state after R14 = %s want normal", mr.State.Severity)
	}
	verifyAgainstReference(ctx, t, st, sr.ID)
}

// TestBackfillBeforeFlagBoundaryUsesOldFlags: a batch backdated to a time
// before the flag boundary is judged under the flags in effect at that point
// (both off), even though the current flags are on.
func TestBackfillBeforeFlagBoundaryUsesOldFlags(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	nID, tID, rID := mkBoundaryPlans(t, st)
	sr, _ := st.CreateStream(ctx, store.CreateStreamParams{
		Name: "backfill", NormalPlanID: nID, TightenedPlanID: tID, ReducedPlanID: rID,
	})
	for i := 1; i <= 12; i++ {
		if _, err := st.AddBatch(ctx, sr.ID, store.BatchInput{
			BatchNo: "B" + itoa2(i), InspectedAt: atDay(i, 0), D1: 0,
		}); err != nil {
			t.Fatal(err)
		}
	}
	on := true
	if _, err := st.SetFlags(ctx, sr.ID, &on, &on); err != nil {
		t.Fatal(err)
	}
	// Backfill at 11:30 — strictly before the boundary (B12 @12:00).
	mr, err := st.AddBatch(ctx, sr.ID, store.BatchInput{
		BatchNo: "B11B", InspectedAt: atDay(11, 30), D1: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	late := lotByNo(t, mr.All, "B11B")
	if late.Result["severity"] != "normal" || planIDOf(late) != nID {
		t.Fatalf("backfilled lot judged severity=%v plan=%d, want normal/%d (flags were off at 11:30)",
			late.Result["severity"], planIDOf(late), nID)
	}
	// B12, at/after the backfill but still before the flags event, is also
	// still normal.
	b12 := lotByNo(t, mr.All, "B12")
	if b12.Result["severity"] != "normal" {
		t.Fatalf("B12 severity=%v, want normal", b12.Result["severity"])
	}
	verifyAgainstReference(ctx, t, st, sr.ID)
}

// TestFlagOnNoBatchesAppliesFromStart covers: flags carried at creation, and
// a PATCH made while the stream still has no batches, both count from the
// very beginning.
func TestFlagOnNoBatchesAppliesFromStart(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	nID, tID, rID := mkBoundaryPlans(t, st)

	t.Run("flags at stream creation", func(t *testing.T) {
		sr, _ := st.CreateStream(ctx, store.CreateStreamParams{
			Name: "from-create", NormalPlanID: nID, TightenedPlanID: tID, ReducedPlanID: rID,
			ProductionStable: true, SupervisorApproval: true,
		})
		var last store.MutationResult
		for i := 1; i <= 10; i++ {
			var err error
			last, err = st.AddBatch(ctx, sr.ID, store.BatchInput{
				BatchNo: "C" + itoa2(i), InspectedAt: atDay(i, 0), D1: 0,
			})
			if err != nil {
				t.Fatal(err)
			}
		}
		c10 := lotByNo(t, last.All, "C10")
		if c10.Result["severity"] != "normal" || c10.Result["transition"] != "to_reduced" {
			t.Fatalf("C10 severity=%v transition=%v, want normal/to_reduced",
				c10.Result["severity"], c10.Result["transition"])
		}
		if last.State.Severity != statemachine.Reduced {
			t.Fatalf("severity=%s", last.State.Severity)
		}
		verifyAgainstReference(ctx, t, st, sr.ID)
	})

	t.Run("PATCH before first batch", func(t *testing.T) {
		sr, _ := st.CreateStream(ctx, store.CreateStreamParams{
			Name: "from-patch", NormalPlanID: nID, TightenedPlanID: tID, ReducedPlanID: rID,
		})
		on := true
		if _, err := st.SetFlags(ctx, sr.ID, &on, &on); err != nil {
			t.Fatal(err)
		}
		var last store.MutationResult
		for i := 1; i <= 10; i++ {
			var err error
			last, err = st.AddBatch(ctx, sr.ID, store.BatchInput{
				BatchNo: "P" + itoa2(i), InspectedAt: atDay(i, 0), D1: 0,
			})
			if err != nil {
				t.Fatal(err)
			}
		}
		p10 := lotByNo(t, last.All, "P10")
		if p10.Result["severity"] != "normal" || p10.Result["transition"] != "to_reduced" {
			t.Fatalf("P10 severity=%v transition=%v, want normal/to_reduced",
				p10.Result["severity"], p10.Result["transition"])
		}
		if last.State.Severity != statemachine.Reduced {
			t.Fatalf("severity=%s", last.State.Severity)
		}
		// Flipping again while batches exist must be bounded by P10..P1 time:
		// P1..P10 records remain, first later lot is normal again.
		off := false
		if _, err := st.SetFlags(ctx, sr.ID, &off, &off); err != nil {
			t.Fatal(err)
		}
		next, err := st.AddBatch(ctx, sr.ID, store.BatchInput{
			BatchNo: "P11", InspectedAt: atDay(11, 0), D1: 0,
		})
		if err != nil {
			t.Fatal(err)
		}
		p11 := lotByNo(t, next.All, "P11")
		if p11.Result["severity"] != "normal" || p11.Result["transition"] != "to_normal" {
			t.Fatalf("P11 severity=%v transition=%v, want normal/to_normal",
				p11.Result["severity"], p11.Result["transition"])
		}
		verifyAgainstReference(ctx, t, st, sr.ID)
	})
}
